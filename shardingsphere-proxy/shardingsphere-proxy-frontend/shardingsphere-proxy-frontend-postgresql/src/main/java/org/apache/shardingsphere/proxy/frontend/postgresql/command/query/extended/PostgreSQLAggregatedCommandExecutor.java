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

package org.apache.shardingsphere.proxy.frontend.postgresql.command.query.extended;

import io.vertx.core.Future;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@RequiredArgsConstructor
public final class PostgreSQLAggregatedCommandExecutor implements CommandExecutor {
    
    private final List<CommandExecutor> executors;
    
    @Override
    public Future<Collection<DatabasePacket<?>>> executeFuture() {
        return doExecuteFuture(new LinkedList<>(), executors.iterator());
    }
    
    private Future<Collection<DatabasePacket<?>>> doExecuteFuture(final Collection<DatabasePacket<?>> result, Iterator<CommandExecutor> iterator) {
        if (!iterator.hasNext()) {
            return Future.succeededFuture(result);
        }
        return iterator.next().executeFuture().compose(executeResult -> {
            result.addAll(executeResult);
            return doExecuteFuture(result, iterator);
        });
    }
    
    @Override
    public Collection<DatabasePacket<?>> execute() throws SQLException {
        List<DatabasePacket<?>> result = new LinkedList<>();
        for (CommandExecutor each : executors) {
            result.addAll(each.execute());
        }
        return result;
    }
    
    @Override
    public void close() throws SQLException {
        Collection<SQLException> exceptions = new LinkedList<>();
        for (CommandExecutor each : executors) {
            try {
                each.close();
            } catch (final SQLException ex) {
                exceptions.add(ex);
            }
        }
        executors.clear();
        if (exceptions.isEmpty()) {
            return;
        }
        SQLException ex = new SQLException();
        exceptions.forEach(ex::setNextException);
        throw ex;
    }
}
