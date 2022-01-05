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

package org.apache.shardingsphere.proxy.frontend.reactive.command.executor;

import io.vertx.core.Future;
import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;

import java.sql.SQLException;
import java.util.Collection;

public interface ReactiveCommandExecutor extends CommandExecutor {
    
    /**
     * Execute command and return future.
     *
     * @return future
     */
    default Future<Collection<DatabasePacket<?>>> executeFuture() {
        try {
            Collection<DatabasePacket<?>> result = execute();
            return Future.succeededFuture(result);
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            return Future.failedFuture(ex);
        }
    }
    
    /**
     * Close future.
     *
     * @return close future
     */
    default Future<Void> closeFuture() {
        try {
            close();
            return Future.succeededFuture();
        } catch (SQLException e) {
            return Future.failedFuture(e);
        }
    }
}
