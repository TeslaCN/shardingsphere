/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.simple;

import io.vertx.core.Future;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.PostgreSQLPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLColumnDescription;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLDataRowPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLEmptyQueryResponsePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLRowDescriptionPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.simple.PostgreSQLComQueryPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.generic.PostgreSQLCommandCompletePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.generic.PostgreSQLReadyForQueryPacket;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.impl.QueryHeader;
import org.apache.shardingsphere.proxy.backend.response.header.update.UpdateResponseHeader;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.text.TextProtocolBackendHandler;
import org.apache.shardingsphere.proxy.backend.text.TextProtocolBackendHandlerFactory;
import org.apache.shardingsphere.proxy.frontend.command.executor.ResponseType;
import org.apache.shardingsphere.proxy.frontend.postgresql.command.PostgreSQLConnectionContext;
import org.apache.shardingsphere.proxy.frontend.postgresql.command.PostgreSQLConnectionContextRegistry;
import org.apache.shardingsphere.proxy.frontend.postgresql.command.query.PostgreSQLCommand;
import org.apache.shardingsphere.proxy.frontend.reactive.command.executor.ReactiveCommandExecutor;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.EmptyStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.tcl.CommitStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.tcl.RollbackStatement;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Reactive command query executor for PostgreSQL.
 */
public final class ReactivePostgreSQLComQueryExecutor implements ReactiveCommandExecutor {
    
    private final PostgreSQLConnectionContext connectionContext;
    
    private final TextProtocolBackendHandler textProtocolBackendHandler;
    
    private final ConnectionSession connectionSession;
    
    @Getter
    private volatile ResponseType responseType;
    
    @SneakyThrows(SQLException.class)
    public ReactivePostgreSQLComQueryExecutor(final PostgreSQLComQueryPacket comQueryPacket, final ConnectionSession connectionSession) {
        this.connectionSession = connectionSession;
        connectionContext = PostgreSQLConnectionContextRegistry.getInstance().get(connectionSession.getConnectionId());
        textProtocolBackendHandler = TextProtocolBackendHandlerFactory.newInstance(DatabaseTypeRegistry.getActualDatabaseType("PostgreSQL"),
                comQueryPacket.getSql(), Optional::empty, connectionSession);
    }
    
    @Override
    public Future<Collection<DatabasePacket<?>>> executeFuture() {
        return textProtocolBackendHandler.executeFuture().compose(responseHeader -> {
            try {
                List<DatabasePacket<?>> result = new LinkedList<>();
                if (!(responseHeader instanceof QueryResponseHeader)) {
                    responseType = ResponseType.UPDATE;
                    result.add(createUpdatePacket((UpdateResponseHeader) responseHeader));
                } else {
                    result.add(createRowDescriptionPacket((QueryResponseHeader) responseHeader));
                    long dataRows = 0;
                    while (textProtocolBackendHandler.next()) {
                        dataRows++;
                        result.add(new PostgreSQLDataRowPacket(textProtocolBackendHandler.getRowData()));
                    }
                    result.add(new PostgreSQLCommandCompletePacket(PostgreSQLCommand.SELECT.name(), dataRows));
                }
                result.add(new PostgreSQLReadyForQueryPacket(connectionSession.getTransactionStatus().isInTransaction()));
                return Future.succeededFuture(result);
            } catch (final SQLException ex) {
                return Future.failedFuture(ex);
            }
        });
    }
    
    private PostgreSQLRowDescriptionPacket createRowDescriptionPacket(final QueryResponseHeader queryResponseHeader) {
        Collection<PostgreSQLColumnDescription> columnDescriptions = createColumnDescriptions(queryResponseHeader);
        responseType = ResponseType.QUERY;
        return new PostgreSQLRowDescriptionPacket(columnDescriptions.size(), columnDescriptions);
    }
    
    private Collection<PostgreSQLColumnDescription> createColumnDescriptions(final QueryResponseHeader queryResponseHeader) {
        Collection<PostgreSQLColumnDescription> result = new LinkedList<>();
        int columnIndex = 0;
        for (QueryHeader each : queryResponseHeader.getQueryHeaders()) {
            result.add(new PostgreSQLColumnDescription(each.getColumnLabel(), ++columnIndex, each.getColumnType(), each.getColumnLength(), each.getColumnTypeName()));
        }
        return result;
    }
    
    private PostgreSQLPacket createUpdatePacket(final UpdateResponseHeader updateResponseHeader) throws SQLException {
        SQLStatement sqlStatement = updateResponseHeader.getSqlStatement();
        if (sqlStatement instanceof CommitStatement || sqlStatement instanceof RollbackStatement) {
            connectionContext.closeAllPortals();
        }
        return sqlStatement instanceof EmptyStatement ? new PostgreSQLEmptyQueryResponsePacket()
                : new PostgreSQLCommandCompletePacket(PostgreSQLCommand.valueOf(sqlStatement.getClass()).map(PostgreSQLCommand::getTag).orElse(""), updateResponseHeader.getUpdateCount());
    }
    
    @Override
    public Future<Void> closeFuture() {
        try {
            textProtocolBackendHandler.close();
            return Future.succeededFuture();
        } catch (final SQLException ex) {
            return Future.failedFuture(ex);
        }
    }
}
