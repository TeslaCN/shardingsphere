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

import lombok.SneakyThrows;
import org.apache.shardingsphere.db.protocol.postgresql.codec.PostgreSQLPacketCodecEngine;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.authentication.AuthenticationEngine;
import org.apache.shardingsphere.proxy.frontend.command.CommandExecuteEngine;
import org.apache.shardingsphere.proxy.frontend.context.FrontendContext;
import org.apache.shardingsphere.proxy.frontend.postgresql.PostgreSQLFrontendEngine;
import org.apache.shardingsphere.proxy.frontend.reactive.command.ReactiveCommandExecuteEngine;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class ReactivePostgreSQLFrontendEngineTest {
    
    private final ReactivePostgreSQLFrontendEngine engine = new ReactivePostgreSQLFrontendEngine();
    
    private final PostgreSQLFrontendEngine delegated = mock(PostgreSQLFrontendEngine.class);
    
    @Before
    @SneakyThrows
    public void setup() {
        Field field = ReactivePostgreSQLFrontendEngine.class.getDeclaredField("delegated");
        field.setAccessible(true);
        Field modifiers = Field.class.getDeclaredField("modifiers");
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(engine, delegated);
    }
    
    @Test
    public void assertGetDatabaseType() {
        when(delegated.getDatabaseType()).thenReturn("expected");
        assertThat(engine.getDatabaseType(), is("expected"));
    }
    
    @Test
    public void assertGetFrontendContext() {
        FrontendContext expected = mock(FrontendContext.class);
        when(delegated.getFrontendContext()).thenReturn(expected);
        assertThat(engine.getFrontendContext(), is(expected));
    }
    
    @Test
    public void assertGetCodecEngine() {
        PostgreSQLPacketCodecEngine expected = mock(PostgreSQLPacketCodecEngine.class);
        when(delegated.getCodecEngine()).thenReturn(expected);
        assertThat(engine.getCodecEngine(), is(expected));
    }
    
    @Test
    public void assertGetAuthenticationEngine() {
        AuthenticationEngine expected = mock(AuthenticationEngine.class);
        when(delegated.getAuthenticationEngine()).thenReturn(expected);
        assertThat(engine.getAuthenticationEngine(), is(expected));
    }
    
    @Test
    public void assertGetCommandExecuteEngine() {
        CommandExecuteEngine expected = mock(CommandExecuteEngine.class);
        when(delegated.getCommandExecuteEngine()).thenReturn(expected);
        assertThat(engine.getCommandExecuteEngine(), is(expected));
    }
    
    @Test
    public void assertRelease() {
        ConnectionSession connectionSession = mock(ConnectionSession.class);
        engine.release(connectionSession);
        verify(delegated).release(connectionSession);
    }
    
    @Test
    @SneakyThrows
    public void getReactiveCommandExecuteEngine() {
        Field field = ReactivePostgreSQLFrontendEngine.class.getDeclaredField("reactiveCommandExecuteEngine");
        field.setAccessible(true);
        ReactiveCommandExecuteEngine expected = (ReactiveCommandExecuteEngine) field.get(engine);
        assertThat(engine.getReactiveCommandExecuteEngine(), is(expected));
    }
}
