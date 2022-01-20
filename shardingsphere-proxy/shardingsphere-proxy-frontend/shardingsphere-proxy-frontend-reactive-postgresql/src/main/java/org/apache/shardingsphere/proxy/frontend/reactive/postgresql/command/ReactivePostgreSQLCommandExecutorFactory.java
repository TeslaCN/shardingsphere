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

package org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.db.protocol.packet.CommandPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.PostgreSQLCommandPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.PostgreSQLCommandPacketType;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLAggregatedCommandPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.bind.PostgreSQLComBindPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.simple.PostgreSQLComQueryPacket;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.postgresql.command.PostgreSQLCommandExecutorFactory;
import org.apache.shardingsphere.proxy.frontend.postgresql.command.PostgreSQLConnectionContext;
import org.apache.shardingsphere.proxy.frontend.postgresql.command.PostgreSQLConnectionContextRegistry;
import org.apache.shardingsphere.proxy.frontend.reactive.command.executor.ReactiveCommandExecutor;
import org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.extended.PostgreSQLAggregatedReactiveCommandExecutor;
import org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.extended.bind.ReactivePostgreSQLComBindExecutor;
import org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.simple.ReactivePostgreSQLComQueryExecutor;
import org.apache.shardingsphere.proxy.frontend.reactive.wrap.WrappedReactiveCommandExecutor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reactive command executor factory for PostgreSQL.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ReactivePostgreSQLCommandExecutorFactory {
    
    /**
     * Create new instance of reactive packet executor.
     *
     * @param commandPacketType command packet type for MySQL
     * @param commandPacket command packet for MySQL
     * @param connectionSession connection session
     * @return command executor
     */
    public static ReactiveCommandExecutor newInstance(final PostgreSQLCommandPacketType commandPacketType, final CommandPacket commandPacket, final ConnectionSession connectionSession) {
        log.debug("Execute packet type: {}, value: {}", commandPacketType, commandPacket);
        PostgreSQLConnectionContext connectionContext = PostgreSQLConnectionContextRegistry.getInstance().get(connectionSession.getConnectionId());
        if (!(commandPacket instanceof PostgreSQLAggregatedCommandPacket)) {
            return getReactiveCommandExecutor(commandPacketType, (PostgreSQLCommandPacket) commandPacket, connectionSession, connectionContext);
        }
        PostgreSQLAggregatedCommandPacket aggregatedCommandPacket = (PostgreSQLAggregatedCommandPacket) commandPacket;
        // TODO Batched inserts using Vert.x backend
        List<ReactiveCommandExecutor> result = new ArrayList<>(aggregatedCommandPacket.getPackets().size());
        for (PostgreSQLCommandPacket each : aggregatedCommandPacket.getPackets()) {
            result.add(getReactiveCommandExecutor((PostgreSQLCommandPacketType) each.getIdentifier(), each, connectionSession, connectionContext));
        }
        return new PostgreSQLAggregatedReactiveCommandExecutor(result);
    }
    
    @SneakyThrows(SQLException.class)
    private static ReactiveCommandExecutor getReactiveCommandExecutor(final PostgreSQLCommandPacketType commandPacketType, final PostgreSQLCommandPacket commandPacket,
                                                                      final ConnectionSession connectionSession, final PostgreSQLConnectionContext connectionContext) {
        switch (commandPacketType) {
            case SIMPLE_QUERY:
                return new ReactivePostgreSQLComQueryExecutor((PostgreSQLComQueryPacket) commandPacket, connectionSession);
            case BIND_COMMAND:
                return new ReactivePostgreSQLComBindExecutor(connectionContext, (PostgreSQLComBindPacket) commandPacket, connectionSession);
            default:
                return new WrappedReactiveCommandExecutor(PostgreSQLCommandExecutorFactory.newInstance(commandPacketType, commandPacket, connectionSession, connectionContext));
        }
    }
}
