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
import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.proxy.frontend.reactive.command.executor.ReactiveCommandExecutor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class PostgreSQLAggregatedReactiveCommandExecutorTest {
    
    @Test
    public void assertExecuteFuture() {
        final int commandCount = 16;
        List<ReactiveCommandExecutor> mockExecutors = new ArrayList<>(commandCount);
        List<DatabasePacket<?>> expectPackets = new ArrayList<>(commandCount);
        for (int i = 0; i < commandCount; i++) {
            ReactiveCommandExecutor executor = mock(ReactiveCommandExecutor.class);
            DatabasePacket<?> packet = mock(DatabasePacket.class);
            when(executor.executeFuture()).thenReturn(Future.succeededFuture(Collections.singletonList(packet)));
            when(executor.closeFuture()).thenReturn(Future.succeededFuture());
            mockExecutors.add(executor);
            expectPackets.add(packet);
        }
        PostgreSQLAggregatedReactiveCommandExecutor executor = new PostgreSQLAggregatedReactiveCommandExecutor(mockExecutors);
        List<DatabasePacket<?>> actualPackets = new ArrayList<>(executor.executeFuture().result());
        for (int i = 0; i < commandCount; i++) {
            DatabasePacket<?> actualPacket = actualPackets.get(i);
            assertThat(actualPacket, is(expectPackets.get(i)));
        }
    }
}
