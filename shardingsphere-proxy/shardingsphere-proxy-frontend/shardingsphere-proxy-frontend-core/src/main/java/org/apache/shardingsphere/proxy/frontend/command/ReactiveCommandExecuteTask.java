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

package org.apache.shardingsphere.proxy.frontend.command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.vertx.core.Future;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.db.protocol.CommonConstants;
import org.apache.shardingsphere.db.protocol.packet.CommandPacket;
import org.apache.shardingsphere.db.protocol.packet.CommandPacketType;
import org.apache.shardingsphere.db.protocol.packet.DatabasePacket;
import org.apache.shardingsphere.db.protocol.payload.PacketPayload;
import org.apache.shardingsphere.proxy.backend.communication.SQLStatementSchemaHolder;
import org.apache.shardingsphere.proxy.backend.exception.BackendConnectionException;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.command.executor.CommandExecutor;
import org.apache.shardingsphere.proxy.frontend.exception.ExpectedExceptions;
import org.apache.shardingsphere.proxy.frontend.spi.DatabaseProtocolFrontendEngine;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reactive command executor task.
 */
@Slf4j
@RequiredArgsConstructor
public final class ReactiveCommandExecuteTask implements Runnable {
    
    private final DatabaseProtocolFrontendEngine databaseProtocolFrontendEngine;
    
    private final ConnectionSession connectionSession;
    
    private final ChannelHandlerContext context;
    
    private final Object message;
    
    private final AtomicBoolean isNeedFlush = new AtomicBoolean(false);
    
    @Override
    public void run() {
        PacketPayload payload = databaseProtocolFrontendEngine.getCodecEngine().createPacketPayload((ByteBuf) message, context.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).get());
        try {
            connectionSession.getBackendConnection().prepareForTaskExecution();
            executeCommand(payload).eventually(unused -> closeResources(payload));
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            processException(ex);
            closeResources(payload);
        }
    }
    
    private Future<Void> executeCommand(final PacketPayload payload) throws SQLException {
        CommandExecuteEngine commandExecuteEngine = databaseProtocolFrontendEngine.getCommandExecuteEngine();
        CommandPacketType type = commandExecuteEngine.getCommandPacketType(payload);
        CommandPacket commandPacket = commandExecuteEngine.getCommandPacket(payload, type, connectionSession);
        CommandExecutor commandExecutor = commandExecuteEngine.getCommandExecutor(type, commandPacket, connectionSession);
        return commandExecutor.executeFuture()
                .compose(this::handleResponsePackets)
                .eventually(unused -> commandExecutor.closeFuture())
                .onFailure(this::processThrowable);
    }
    
    private Future<Void> handleResponsePackets(Collection<DatabasePacket<?>> responsePackets) {
        log.info("Connection {} handling {} response packets.", connectionSession.getConnectionId(), responsePackets.size());
        responsePackets.forEach(context::write);
        isNeedFlush.set(!responsePackets.isEmpty());
        return Future.succeededFuture();
    }
    
    @SuppressWarnings("unchecked")
    @SneakyThrows(BackendConnectionException.class)
    private Future<Void> closeResources(final PacketPayload payload) {
        try {
            payload.close();
        } catch (final Exception ignored) {
        }
        SQLStatementSchemaHolder.remove();
        return ((Future<Void>) connectionSession.getBackendConnection().closeExecutionResources()).eventually(this::doFlushIfNecessary);
    }
    
    private Future<Void> doFlushIfNecessary(final Void unused) {
        log.info("Connection {} isNeedFlush {}", connectionSession.getConnectionId(), isNeedFlush);
        if (isNeedFlush.get()) {
            context.flush();
        }
        return Future.succeededFuture();
    }
    
    private void processThrowable(final Throwable throwable) {
        Exception ex = throwable instanceof Exception ? (Exception) throwable : new Exception(throwable);
        processException(ex);
    }
    
    private void processException(final Exception cause) {
        if (!ExpectedExceptions.isExpected(cause.getClass())) {
            log.error("Exception occur: ", cause);
        }
        context.write(databaseProtocolFrontendEngine.getCommandExecuteEngine().getErrorPacket(cause, connectionSession));
        Optional<DatabasePacket<?>> databasePacket = databaseProtocolFrontendEngine.getCommandExecuteEngine().getOtherPacket(connectionSession);
        databasePacket.ifPresent(context::write);
        context.flush();
    }
}
