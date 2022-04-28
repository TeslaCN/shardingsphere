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

package org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.extended;

import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.apache.shardingsphere.db.protocol.postgresql.constant.PostgreSQLValueFormat;
import org.apache.shardingsphere.db.protocol.postgresql.packet.PostgreSQLPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLDataRowPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLEmptyQueryResponsePacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLNoDataPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.PostgreSQLRowDescriptionPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLPreparedStatement;
import org.apache.shardingsphere.db.protocol.postgresql.packet.generic.PostgreSQLCommandCompletePacket;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.proxy.backend.communication.vertx.VertxBackendConnection;
import org.apache.shardingsphere.proxy.backend.communication.vertx.VertxDatabaseCommunicationEngine;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.response.data.QueryResponseRow;
import org.apache.shardingsphere.proxy.backend.response.data.impl.TextQueryResponseCell;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.QueryResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.update.UpdateResponseHeader;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.backend.text.TextProtocolBackendHandler;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.EmptyStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class ReactivePortalTest {
    
    private ContextManager contextManagerBefore;
    
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ContextManager mockContextManager;
    
    @Mock
    private VertxDatabaseCommunicationEngine databaseCommunicationEngine;
    
    @Mock
    private TextProtocolBackendHandler textProtocolBackendHandler;
    
    @Mock
    private ConnectionSession connectionSession;
    
    @Mock
    private VertxBackendConnection backendConnection;
    
    private ReactivePortal portal;
    
    @Before
    public void setup() {
        contextManagerBefore = ProxyContext.getInstance().getContextManager();
        ProxyContext.getInstance().init(mockContextManager);
        when(ProxyContext.getInstance().getContextManager().getMetaDataContexts().getProps().getValue(ConfigurationPropertyKey.SQL_SHOW)).thenReturn(false);
        when(backendConnection.getConnectionSession()).thenReturn(connectionSession);
        prepareReactivePortal();
    }
    
    private void prepareReactivePortal() {
        PostgreSQLPreparedStatement preparedStatement = mock(PostgreSQLPreparedStatement.class);
        when(preparedStatement.getSql()).thenReturn("");
        when(preparedStatement.getSqlStatement()).thenReturn(new EmptyStatement());
        List<PostgreSQLValueFormat> resultFormats = new ArrayList<>(Arrays.asList(PostgreSQLValueFormat.TEXT, PostgreSQLValueFormat.BINARY));
        portal = new ReactivePortal("", preparedStatement, Collections.emptyList(), resultFormats, backendConnection);
    }
    
    @Test
    public void assertGetName() {
        assertThat(portal.getName(), is(""));
    }
    
    @Test
    public void assertExecuteSelectStatementWithDatabaseCommunicationEngineAndReturnAllRows() throws SQLException {
        setField(portal, "databaseCommunicationEngine", databaseCommunicationEngine);
        setField(portal, "textProtocolBackendHandler", null);
        QueryResponseHeader responseHeader = mock(QueryResponseHeader.class);
        QueryHeader queryHeader = new QueryHeader("schema", "table", "columnLabel", "columnName", Types.INTEGER, "columnTypeName", 0, 0, false, false, false, false);
        when(responseHeader.getQueryHeaders()).thenReturn(Collections.singletonList(queryHeader));
        when(databaseCommunicationEngine.execute()).thenReturn(Future.succeededFuture(responseHeader));
        when(databaseCommunicationEngine.next()).thenReturn(true, true, false);
        when(databaseCommunicationEngine.getQueryResponseRow())
                .thenReturn(new QueryResponseRow(Collections.singletonList(new TextQueryResponseCell(0))), new QueryResponseRow(Collections.singletonList(new TextQueryResponseCell(1))));
        assertNull(portal.bind().result());
        assertTrue(portal.describe() instanceof PostgreSQLRowDescriptionPacket);
        setField(portal, "sqlStatement", mock(SelectStatement.class));
        List<PostgreSQLPacket> actualPackets = portal.execute(0);
        assertThat(actualPackets.size(), is(3));
        Iterator<PostgreSQLPacket> actualPacketsIterator = actualPackets.iterator();
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLDataRowPacket);
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLDataRowPacket);
        assertTrue(actualPacketsIterator.next() instanceof PostgreSQLCommandCompletePacket);
    }
    
    @Test
    public void assertExecuteUpdateWithDatabaseCommunicationEngine() throws SQLException {
        setField(portal, "databaseCommunicationEngine", databaseCommunicationEngine);
        setField(portal, "textProtocolBackendHandler", null);
        when(databaseCommunicationEngine.execute()).thenReturn(Future.succeededFuture(mock(UpdateResponseHeader.class)));
        when(databaseCommunicationEngine.next()).thenReturn(false);
        assertNull(portal.bind().result());
        assertThat(portal.describe(), is(PostgreSQLNoDataPacket.getInstance()));
        setField(portal, "sqlStatement", mock(InsertStatement.class));
        List<PostgreSQLPacket> actualPackets = portal.execute(0);
        assertTrue(actualPackets.iterator().next() instanceof PostgreSQLCommandCompletePacket);
    }
    
    @Test
    public void assertExecuteEmptyStatementWithDatabaseCommunicationEngine() throws SQLException {
        setField(portal, "databaseCommunicationEngine", databaseCommunicationEngine);
        setField(portal, "textProtocolBackendHandler", null);
        when(databaseCommunicationEngine.execute()).thenReturn(Future.succeededFuture(mock(UpdateResponseHeader.class)));
        when(databaseCommunicationEngine.next()).thenReturn(false);
        assertNull(portal.bind().result());
        assertThat(portal.describe(), is(PostgreSQLNoDataPacket.getInstance()));
        List<PostgreSQLPacket> actualPackets = portal.execute(0);
        assertTrue(actualPackets.iterator().next() instanceof PostgreSQLEmptyQueryResponsePacket);
    }
    
    @Test(expected = IllegalStateException.class)
    public void assertDescribeBeforeBind() {
        PostgreSQLPreparedStatement preparedStatement = mock(PostgreSQLPreparedStatement.class);
        when(preparedStatement.getSql()).thenReturn("");
        when(preparedStatement.getSqlStatement()).thenReturn(new EmptyStatement());
        new ReactivePortal("", preparedStatement, Collections.emptyList(), Collections.emptyList(), backendConnection).describe();
    }
    
    @Test
    public void assertClose() throws SQLException {
        setField(portal, "databaseCommunicationEngine", databaseCommunicationEngine);
        setField(portal, "textProtocolBackendHandler", textProtocolBackendHandler);
        portal.close();
        verify(textProtocolBackendHandler).close();
    }
    
    @SneakyThrows
    private void setField(final ReactivePortal portal, final String fieldName, final Object value) {
        Field field = ReactivePortal.class.getDeclaredField(fieldName);
        makeAccessible(field);
        field.set(portal, value);
    }
    
    @SneakyThrows
    private void makeAccessible(final Field field) {
        field.setAccessible(true);
        Field modifiersField = getModifiersField();
        modifiersField.setAccessible(true);
        modifiersField.set(field, field.getModifiers() & ~Modifier.FINAL);
    }
    
    @SneakyThrows
    private Field getModifiersField() {
        Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
        getDeclaredFields0.setAccessible(true);
        Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false);
        for (Field each : fields) {
            if ("modifiers".equals(each.getName())) {
                return each;
            }
        }
        throw new UnsupportedOperationException();
    }
    
    @After
    public void tearDown() {
        ProxyContext.getInstance().init(contextManagerBefore);
    }
}
