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

import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLPreparedStatementRegistry;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.bind.PostgreSQLBindCompletePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.bind.PostgreSQLComBindPacket;
import org.apache.shardingsphere.proxy.backend.communication.vertx.VertxBackendConnection;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.postgresql.command.PostgreSQLConnectionContext;
import org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.extended.ReactivePortal;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.EmptyStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class ReactivePostgreSQLComBindExecutorTest {
    
    @Mock
    private PostgreSQLConnectionContext connectionContext;
    
    @Mock
    private PostgreSQLComBindPacket bindPacket;
    
    @Mock
    private ConnectionSession connectionSession;
    
    @InjectMocks
    private ReactivePostgreSQLComBindExecutor executor;
    
    @Before
    public void setup() {
        PostgreSQLPreparedStatementRegistry.getInstance().register(1);
        PostgreSQLPreparedStatementRegistry.getInstance().register(1, "2", "", new EmptyStatement(), Collections.emptyList());
        when(bindPacket.getStatementId()).thenReturn("1");
        when(bindPacket.getPortal()).thenReturn("C_1");
        when(bindPacket.readParameters(anyList())).thenReturn(Collections.emptyList());
        when(bindPacket.readResultFormats()).thenReturn(Collections.emptyList());
        when(connectionSession.getConnectionId()).thenReturn(1);
        VertxBackendConnection backendConnection = mock(VertxBackendConnection.class);
        when(connectionSession.getBackendConnection()).thenReturn(backendConnection);
    }
    
    @Test
    public void assertExecuteEmptyBindPacket() {
        Collection<DatabasePacket<?>> actual = executor.executeFuture().result();
        assertThat(actual.size(), is(1));
        assertThat(actual.iterator().next(), is(PostgreSQLBindCompletePacket.getInstance()));
        verify(connectionContext).addPortal(any(ReactivePortal.class));
    }
    
    @Test
    public void assertExecuteBindPacketWithQuerySQLAndReturnEmptyResult() {
        Collection<DatabasePacket<?>> actual = executor.executeFuture().result();
        assertThat(actual.size(), is(1));
        assertThat(actual.iterator().next(), is(PostgreSQLBindCompletePacket.getInstance()));
        verify(connectionContext).addPortal(any(ReactivePortal.class));
    }
    
    @Test
    public void assertExecuteBindPacketWithQuerySQL() {
        Collection<DatabasePacket<?>> actual = executor.executeFuture().result();
        assertThat(actual.size(), is(1));
        Iterator<DatabasePacket<?>> actualPackets = actual.iterator();
        assertThat(actualPackets.next(), is(PostgreSQLBindCompletePacket.getInstance()));
        verify(connectionContext).addPortal(any(ReactivePortal.class));
    }
    
    @Test
    public void assertExecuteBindPacketWithUpdateSQL() {
        Collection<DatabasePacket<?>> actual = executor.executeFuture().result();
        assertThat(actual.size(), is(1));
        assertThat(actual.iterator().next(), is(PostgreSQLBindCompletePacket.getInstance()));
        verify(connectionContext).addPortal(any(ReactivePortal.class));
    }
}
