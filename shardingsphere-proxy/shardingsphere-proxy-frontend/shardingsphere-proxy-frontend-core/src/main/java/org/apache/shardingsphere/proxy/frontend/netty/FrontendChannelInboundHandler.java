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

package org.apache.shardingsphere.proxy.frontend.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.db.protocol.CommonConstants;
import org.apache.shardingsphere.db.protocol.payload.PacketPayload;
import org.apache.shardingsphere.infra.config.properties.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.metadata.user.Grantee;
import org.apache.shardingsphere.proxy.backend.communication.BackendConnection;
import org.apache.shardingsphere.proxy.backend.communication.jdbc.connection.JDBCBackendConnection;
import org.apache.shardingsphere.proxy.backend.communication.vertx.VertxBackendConnection;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.exception.BackendConnectionException;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;
import org.apache.shardingsphere.proxy.frontend.authentication.AuthenticationResult;
import org.apache.shardingsphere.proxy.frontend.executor.ConnectionThreadExecutorGroup;
import org.apache.shardingsphere.proxy.frontend.spi.DatabaseProtocolFrontendEngine;
import org.apache.shardingsphere.proxy.frontend.state.ProxyStateContext;
import org.apache.shardingsphere.transaction.rule.TransactionRule;
import org.apache.shardingsphere.transaction.rule.builder.DefaultTransactionRuleConfigurationBuilder;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Frontend channel inbound handler.
 */
@Slf4j
public final class FrontendChannelInboundHandler extends ChannelInboundHandlerAdapter {
    
    private static final Map<ChannelId, SocketChannel> CHANNELS = new ConcurrentHashMap<>();
    
    static {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (SocketChannel each : CHANNELS.values()) {
                log.info("Flushing {}", each);
                each.flush();
            }
        });
    }
    
    private final DatabaseProtocolFrontendEngine databaseProtocolFrontendEngine;
    
    private final ConnectionSession connectionSession;
    
    private volatile boolean authenticated;
    
    public FrontendChannelInboundHandler(final DatabaseProtocolFrontendEngine databaseProtocolFrontendEngine, final Channel channel) {
        this.databaseProtocolFrontendEngine = databaseProtocolFrontendEngine;
        connectionSession = new ConnectionSession(getTransactionRule().getDefaultType(), channel);
        // TODO Decouple JDBCBackendConnection from this class.
        boolean reactiveBackendEnabled = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getProps().getValue(ConfigurationPropertyKey.EXPERIMENTAL_REACTIVE_BACKEND_ENABLED);
        BackendConnection backendConnection = reactiveBackendEnabled ? new VertxBackendConnection(connectionSession) : new JDBCBackendConnection(connectionSession);
        connectionSession.setBackendConnection(backendConnection);
        CHANNELS.put(channel.id(), (SocketChannel) channel);
    }
    
    private TransactionRule getTransactionRule() {
        Optional<TransactionRule> transactionRule = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getGlobalRuleMetaData().getRules().stream().filter(
            each -> each instanceof TransactionRule).map(each -> (TransactionRule) each).findFirst();
        return transactionRule.orElseGet(() -> new TransactionRule(new DefaultTransactionRuleConfigurationBuilder().build()));
    }
    
    @Override
    public void channelActive(final ChannelHandlerContext context) {
        int connectionId = databaseProtocolFrontendEngine.getAuthenticationEngine().handshake(context);
        ConnectionThreadExecutorGroup.getInstance().register(connectionId);
        connectionSession.setConnectionId(connectionId);
    }
    
    @Override
    public void channelRead(final ChannelHandlerContext context, final Object message) {
        if (!authenticated) {
            authenticated = authenticate(context, (ByteBuf) message);
            return;
        }
        ProxyStateContext.execute(context, message, databaseProtocolFrontendEngine, connectionSession);
    }
    
    private boolean authenticate(final ChannelHandlerContext context, final ByteBuf message) {
        try (PacketPayload payload = databaseProtocolFrontendEngine.getCodecEngine().createPacketPayload(message, context.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).get())) {
            AuthenticationResult authResult = databaseProtocolFrontendEngine.getAuthenticationEngine().authenticate(context, payload);
            if (authResult.isFinished()) {
                connectionSession.setGrantee(new Grantee(authResult.getUsername(), authResult.getHostname()));
                connectionSession.setCurrentSchema(authResult.getDatabase());
            }
            return authResult.isFinished();
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            log.error("Exception occur: ", ex);
            context.writeAndFlush(databaseProtocolFrontendEngine.getCommandExecuteEngine().getErrorPacket(ex, connectionSession));
            context.close();
        }
        return false;
    }
    
    @Override
    public void channelInactive(final ChannelHandlerContext context) {
        CHANNELS.remove(context.channel().id());
        context.fireChannelInactive();
        closeAllResources();
    }
    
    private void closeAllResources() {
        ConnectionThreadExecutorGroup.getInstance().unregisterAndAwaitTermination(connectionSession.getConnectionId());
        try {
            connectionSession.getBackendConnection().closeAllResources();
        } catch (final BackendConnectionException ex) {
            log.error("Exception occurred when frontend connection [{}] disconnected", connectionSession.getConnectionId(), ex);
        }
        databaseProtocolFrontendEngine.release(connectionSession);
    }
    
    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext context) {
        if (context.channel().isWritable()) {
            ((JDBCBackendConnection) connectionSession.getBackendConnection()).getResourceLock().doNotify();
        }
    }
}
