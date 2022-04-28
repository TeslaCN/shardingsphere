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
import lombok.SneakyThrows;
import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLDataRowPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLEmptyQueryResponsePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLRowDescriptionPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.simple.PostgreSQLComQueryPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.generic.PostgreSQLCommandCompletePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.generic.PostgreSQLReadyForQueryPacket;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.update.UpdateResponseHeader;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.text.TextProtocolBackendHandler;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.EmptyStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.InsertStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class ReactivePostgreSQLComQueryExecutorTest {
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConnectionSession connectionSession;
    
    private ReactivePostgreSQLComQueryExecutor executor;
    
    @Mock
    private TextProtocolBackendHandler textProtocolBackendHandler;
    
    @Before
    public void setUp() {
        PostgreSQLComQueryPacket packet = mock(PostgreSQLComQueryPacket.class);
        when(packet.getSql()).thenReturn("");
        executor = new ReactivePostgreSQLComQueryExecutor(packet, connectionSession);
        setMockFieldIntoExecutor();
    }
    
    @SneakyThrows
    private void setMockFieldIntoExecutor() {
        Field field = ReactivePostgreSQLComQueryExecutor.class.getDeclaredField("textProtocolBackendHandler");
        field.setAccessible(true);
        Field modifiers = Field.class.getDeclaredField("modifiers");
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(executor, textProtocolBackendHandler);
    }
    
    @Test
    public void assertExecuteQueryAndReturnResult() throws SQLException {
        QueryResponseHeader queryResponseHeader = mock(QueryResponseHeader.class);
        when(queryResponseHeader.getQueryHeaders()).thenReturn(Collections.singletonList(new QueryHeader("schema", "table", "label", "column", 1, "type", 2, 3, true, true, true, true)));
        when(textProtocolBackendHandler.executeFuture()).thenReturn(Future.succeededFuture(queryResponseHeader));
        when(textProtocolBackendHandler.next()).thenReturn(true, false);
        when(textProtocolBackendHandler.getRowData()).thenReturn(Collections.emptyList());
        Iterator<DatabasePacket<?>> actualPacketsIterator = executor.executeFuture().result().iterator();
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLRowDescriptionPacket);
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLDataRowPacket);
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLCommandCompletePacket);
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLReadyForQueryPacket);
        assertFalse(actualPacketsIterator.hasNext());
    }
    
    @Test
    public void assertExecuteEmptyQuery() {
        UpdateResponseHeader updateResponseHeader = mock(UpdateResponseHeader.class);
        when(updateResponseHeader.getSqlStatement()).thenReturn(new EmptyStatement());
        when(textProtocolBackendHandler.executeFuture()).thenReturn(Future.succeededFuture(updateResponseHeader));
        Iterator<DatabasePacket<?>> actualPacketsIterator = executor.executeFuture().result().iterator();
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLEmptyQueryResponsePacket);
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLReadyForQueryPacket);
        assertFalse(actualPacketsIterator.hasNext());
    }
    
    @Test
    public void assertExecuteUpdate() {
        when(textProtocolBackendHandler.executeFuture()).thenReturn(Future.succeededFuture(new UpdateResponseHeader(mock(InsertStatement.class))));
        Iterator<DatabasePacket<?>> actualPacketsIterator = executor.executeFuture().result().iterator();
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLCommandCompletePacket);
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLReadyForQueryPacket);
        assertFalse(actualPacketsIterator.hasNext());
    }
    
    @Test
    public void assertExecuteAndExceptionOccur() throws SQLException {
        SQLException ex = mock(SQLException.class);
        when(textProtocolBackendHandler.executeFuture()).thenReturn(Future.succeededFuture(mock(QueryResponseHeader.class)));
        when(textProtocolBackendHandler.next()).thenThrow(ex);
        assertThat(executor.executeFuture().cause(), is(ex));
    }
    
    @Test
    public void assertCloseFutureSucceeded() throws SQLException {
        assertThat(executor.closeFuture(), is(Future.succeededFuture()));
        verify(textProtocolBackendHandler).close();
    }
    
    @Test
    public void assertCloseFutureFailed() throws SQLException {
        SQLException ex = mock(SQLException.class);
        doThrow(ex).when(textProtocolBackendHandler).close();
        assertThat(executor.closeFuture().cause(), is(ex));
    }
}
