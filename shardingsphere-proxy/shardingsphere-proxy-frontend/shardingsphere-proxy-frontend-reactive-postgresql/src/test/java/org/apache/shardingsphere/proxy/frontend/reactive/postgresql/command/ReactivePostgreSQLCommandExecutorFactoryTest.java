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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.PostgreSQLCommandPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.PostgreSQLCommandPacketType;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLAggregatedCommandPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLPreparedStatementRegistry;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.bind.PostgreSQLComBindPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.execute.PostgreSQLComExecutePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.parse.PostgreSQLComParsePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.sync.PostgreSQLComSyncPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.simple.PostgreSQLComQueryPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.generic.PostgreSQLComTerminationPacket;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.reactive.command.executor.ReactiveCommandExecutor;
import org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.extended.PostgreSQLAggregatedReactiveCommandExecutor;
import org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.extended.bind.ReactivePostgreSQLComBindExecutor;
import org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.simple.ReactivePostgreSQLComQueryExecutor;
import org.apache.shardingsphere.proxy.frontend.reactive.wrap.WrappedReactiveCommandExecutor;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.EmptyStatement;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class ReactivePostgreSQLCommandExecutorFactoryTest {
    
    private ConnectionSession connectionSession;
    
    @Before
    public void setup() {
        connectionSession = mock(ConnectionSession.class);
        when(connectionSession.getConnectionId()).thenReturn(1);
        PostgreSQLPreparedStatementRegistry.getInstance().register(1);
        PostgreSQLPreparedStatementRegistry.getInstance().register(1, "2", "", new EmptyStatement(), Collections.emptyList());
    }
    
    @Test
    public void assertNewInstance() {
        Collection<InputOutput> inputOutputs = Arrays.asList(
                new InputOutput(PostgreSQLCommandPacketType.SIMPLE_QUERY, PostgreSQLComQueryPacket.class, ReactivePostgreSQLComQueryExecutor.class),
                new InputOutput(PostgreSQLCommandPacketType.BIND_COMMAND, PostgreSQLComBindPacket.class, ReactivePostgreSQLComBindExecutor.class),
                new InputOutput(PostgreSQLCommandPacketType.PARSE_COMMAND, PostgreSQLComParsePacket.class, WrappedReactiveCommandExecutor.class),
                new InputOutput(PostgreSQLCommandPacketType.EXECUTE_COMMAND, PostgreSQLComExecutePacket.class, WrappedReactiveCommandExecutor.class),
                new InputOutput(PostgreSQLCommandPacketType.SYNC_COMMAND, PostgreSQLComSyncPacket.class, WrappedReactiveCommandExecutor.class),
                new InputOutput(PostgreSQLCommandPacketType.TERMINATE, PostgreSQLComTerminationPacket.class, WrappedReactiveCommandExecutor.class),
                new InputOutput(null, PostgreSQLAggregatedCommandPacket.class, PostgreSQLAggregatedReactiveCommandExecutor.class)
        );
        for (InputOutput inputOutput : inputOutputs) {
            Class<? extends PostgreSQLCommandPacket> commandPacketClass = inputOutput.getCommandPacketClass();
            if (null == commandPacketClass) {
                commandPacketClass = PostgreSQLCommandPacket.class;
            }
            PostgreSQLCommandPacket packet = preparePacket(commandPacketClass);
            ReactiveCommandExecutor actual = ReactivePostgreSQLCommandExecutorFactory.newInstance(inputOutput.getCommandPacketType(), packet, connectionSession);
            assertThat(actual, instanceOf(inputOutput.getResultClass()));
        }
    }
    
    private PostgreSQLCommandPacket preparePacket(final Class<? extends PostgreSQLCommandPacket> commandPacketClass) {
        PostgreSQLCommandPacket result = mock(commandPacketClass);
        if (result instanceof PostgreSQLComQueryPacket) {
            when(((PostgreSQLComQueryPacket) result).getSql()).thenReturn("");
        }
        if (result instanceof PostgreSQLAggregatedCommandPacket) {
            PostgreSQLComBindPacket packet = mock(PostgreSQLComBindPacket.class);
            when(packet.getIdentifier()).thenReturn(PostgreSQLCommandPacketType.BIND_COMMAND);
            when(((PostgreSQLAggregatedCommandPacket) result).getPackets()).thenReturn(Collections.singletonList(packet));
        }
        return result;
    }
    
    @RequiredArgsConstructor
    @Getter
    private static final class InputOutput {
        
        private final PostgreSQLCommandPacketType commandPacketType;
        
        private final Class<? extends PostgreSQLCommandPacket> commandPacketClass;
        
        private final Class<? extends ReactiveCommandExecutor> resultClass;
    }
}
