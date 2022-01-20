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

package org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.extended.bind;

import io.vertx.core.Future;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLPreparedStatement;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLPreparedStatementRegistry;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.bind.PostgreSQLBindCompletePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.bind.PostgreSQLComBindPacket;
import org.apache.shardingsphere.proxy.backend.communication.vertx.VertxBackendConnection;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.postgresql.command.PostgreSQLConnectionContext;
import org.apache.shardingsphere.proxy.frontend.reactive.command.executor.ReactiveCommandExecutor;
import org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.extended.ReactivePortal;

import java.util.Collection;
import java.util.Collections;

/**
 * Reactive command bind executor for PostgreSQL.
 */
@RequiredArgsConstructor
public final class ReactivePostgreSQLComBindExecutor implements ReactiveCommandExecutor {
    
    private final PostgreSQLConnectionContext connectionContext;
    
    private final PostgreSQLComBindPacket packet;
    
    private final ConnectionSession connectionSession;
    
    @Override
    public Future<Collection<DatabasePacket<?>>> executeFuture() {
        PostgreSQLPreparedStatement preparedStatement = PostgreSQLPreparedStatementRegistry.getInstance().get(connectionSession.getConnectionId(), packet.getStatementId());
        VertxBackendConnection backendConnection = (VertxBackendConnection) connectionSession.getBackendConnection();
        ReactivePortal portal = new ReactivePortal(packet.getPortal(), preparedStatement, packet.readParameters(preparedStatement.getParameterTypes()), packet.readResultFormats(), backendConnection);
        connectionContext.addPortal(portal);
        return portal.bind().compose(unused -> Future.succeededFuture(Collections.singletonList(PostgreSQLBindCompletePacket.getInstance())));
    }
}
