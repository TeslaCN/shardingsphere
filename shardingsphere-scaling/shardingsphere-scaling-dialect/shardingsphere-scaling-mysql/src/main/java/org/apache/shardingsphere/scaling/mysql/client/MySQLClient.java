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

package org.apache.shardingsphere.scaling.mysql.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.db.protocol.codec.PacketCodec;
import org.apache.shardingsphere.db.protocol.mysql.codec.MySQLPacketCodecEngine;
import org.apache.shardingsphere.db.protocol.mysql.packet.command.binlog.MySQLComBinlogDumpCommandPacket;
import org.apache.shardingsphere.db.protocol.mysql.packet.command.binlog.MySQLComRegisterSlaveCommandPacket;
import org.apache.shardingsphere.db.protocol.mysql.packet.command.query.binary.execute.MySQLComStmtExecutePacket;
import org.apache.shardingsphere.db.protocol.mysql.packet.command.query.binary.prepare.MySQLComStmtPreparePacket;
import org.apache.shardingsphere.db.protocol.mysql.packet.command.query.text.query.MySQLComQueryPacket;
import org.apache.shardingsphere.db.protocol.mysql.packet.generic.MySQLErrPacket;
import org.apache.shardingsphere.db.protocol.mysql.packet.generic.MySQLOKPacket;
import org.apache.shardingsphere.db.protocol.netty.ChannelAttrInitializer;
import org.apache.shardingsphere.scaling.mysql.binlog.event.AbstractBinlogEvent;
import org.apache.shardingsphere.scaling.mysql.client.netty.MySQLBinlogEventPacketDecoder;
import org.apache.shardingsphere.scaling.mysql.client.netty.MySQLCommandPacketDecoder;
import org.apache.shardingsphere.scaling.mysql.client.netty.MySQLNegotiateHandler;
import org.apache.shardingsphere.scaling.mysql.client.netty.MySQLNegotiatePackageDecoder;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * MySQL Connector.
 */
@RequiredArgsConstructor
@Slf4j
public final class MySQLClient {
    
    private final ConnectInfo connectInfo;
    
    private EventLoopGroup eventLoopGroup;
    
    private Channel channel;
    
    private Promise<Object> responseCallback;
    
    private final ArrayBlockingQueue<AbstractBinlogEvent> blockingEventQueue = new ArrayBlockingQueue<>(10000);
    
    private ServerInfo serverInfo;
    
    private final Map<String, Integer> preparedStatements = new HashMap<>(1024);
    
    /**
     * Connect to MySQL.
     */
    public synchronized void connect(final EventLoopGroup eventLoopGroup) {
//        this.eventLoopGroup = eventLoopGroup;
        this.eventLoopGroup = Epoll.isAvailable() ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
        responseCallback = new DefaultPromise<>(this.eventLoopGroup.next());
        channel = new Bootstrap()
                .group(this.eventLoopGroup)
                .channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.AUTO_READ, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    
                    @Override
                    protected void initChannel(final SocketChannel socketChannel) {
                        socketChannel.pipeline().addLast(new ChannelAttrInitializer());
                        socketChannel.pipeline().addLast("PacketCodec", new PacketCodec(new MySQLPacketCodecEngine()));
                        socketChannel.pipeline().addLast(new MySQLNegotiatePackageDecoder());
                        socketChannel.pipeline().addLast(new MySQLCommandPacketDecoder());
                        socketChannel.pipeline().addLast(new MySQLNegotiateHandler(connectInfo.getDatabase(), connectInfo.getUsername(), connectInfo.getPassword(), responseCallback));
//                        socketChannel.pipeline().addLast(new MySQLCommandResponseHandler());
                    }
                }).connect(connectInfo.getHost(), connectInfo.getPort()).channel();
        serverInfo = waitExpectedResponse(ServerInfo.class);
    }
    
    @RequiredArgsConstructor
    private static class MySQLPreparedOKInboundHandler extends ChannelInboundHandlerAdapter {
        
        private final String sql;
        
        private final Map<String, Integer> preparedStatements;
        
        private final Runnable callback;
        
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            ByteBuf byteBuf = (ByteBuf) msg;
            int statementId = byteBuf.getIntLE(2);
            preparedStatements.put(sql, statementId);
            ctx.executor().execute(callback);
            ctx.pipeline().remove(this);
        }
    }
    
    @SneakyThrows
    public void prepareStatement(final String sql, final Runnable callback) {
        if (!preparedStatements.containsKey(sql)) {
            MySQLComStmtPreparePacket preparePacket = new MySQLComStmtPreparePacket(sql);
            channel.pipeline().addAfter("PacketCodec", "PreparedOkHandler", new MySQLPreparedOKInboundHandler(sql, preparedStatements, callback));
            channel.writeAndFlush(preparePacket);
        }
    }
    
    @RequiredArgsConstructor
    private static class MySQLPreparedStatementExecuteInboundHandler extends ChannelInboundHandlerAdapter {
        
        private final Consumer<List<ByteBuf>> callback;
        
        private final List<ByteBuf> results = new LinkedList<>();
    
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            ByteBuf byteBuf = (ByteBuf) msg;
            ByteBuf result = ctx.alloc().buffer(byteBuf.readableBytes() + 3);
            result.writeMediumLE(byteBuf.readableBytes() - 1);
            result.writeBytes(byteBuf);
            byteBuf.release();
            results.add(result);
            if (results.size() > 3 && 0xfe == result.getUnsignedByte(4)) {
                callback.accept(results);
                ctx.pipeline().remove(this);
            }
        }
    }
    
    @SneakyThrows
    public void executePreparedStatement(final MySQLComStmtExecutePacket packet, final Consumer<List<ByteBuf>> callback) {
        String sql = packet.getSql();
        packet.setStatementId(preparedStatements.get(sql));
//        channel.pipeline().addFirst("executePreparedStatement", new MySQLPreparedStatementExecuteInboundHandler(callback));
        channel.pipeline().addAfter("PacketCodec", "executePreparedStatement", new MySQLPreparedStatementExecuteInboundHandler(callback));
        channel.writeAndFlush(packet);
    }
    
    /**
     * Execute command.
     *
     * @param queryString query string
     * @return true if execute successfully, otherwise false
     */
    public synchronized boolean execute(final String queryString) {
        responseCallback = new DefaultPromise<>(eventLoopGroup.next());
        MySQLComQueryPacket comQueryPacket = new MySQLComQueryPacket(queryString);
        channel.writeAndFlush(comQueryPacket);
        return null != waitExpectedResponse(MySQLOKPacket.class);
    }
    
    /**
     * Execute update.
     *
     * @param queryString query string
     * @return affected rows
     */
    public synchronized int executeUpdate(final String queryString) {
        responseCallback = new DefaultPromise<>(eventLoopGroup.next());
        MySQLComQueryPacket comQueryPacket = new MySQLComQueryPacket(queryString);
        channel.writeAndFlush(comQueryPacket);
        return (int) Objects.requireNonNull(waitExpectedResponse(MySQLOKPacket.class)).getAffectedRows();
    }
    
    /**
     * Execute query.
     *
     * @param queryString query string
     * @return result set
     */
    public synchronized InternalResultSet executeQuery(final String queryString) {
        responseCallback = new DefaultPromise<>(eventLoopGroup.next());
        MySQLComQueryPacket comQueryPacket = new MySQLComQueryPacket(queryString);
        channel.writeAndFlush(comQueryPacket);
        return waitExpectedResponse(InternalResultSet.class);
    }
    
    /**
     * Start dump binlog.
     *
     * @param binlogFileName binlog file name
     * @param binlogPosition binlog position
     */
    public synchronized void subscribe(final String binlogFileName, final long binlogPosition) {
        initDumpConnectSession();
        registerSlave();
        dumpBinlog(binlogFileName, binlogPosition, queryChecksumLength());
    }
    
    private void initDumpConnectSession() {
        if (serverInfo.getServerVersion().greaterThanOrEqualTo(5, 6, 0)) {
            execute("SET @MASTER_BINLOG_CHECKSUM= @@GLOBAL.BINLOG_CHECKSUM");
        }
    }
    
    private void registerSlave() {
        responseCallback = new DefaultPromise<>(eventLoopGroup.next());
        InetSocketAddress localAddress = (InetSocketAddress) channel.localAddress();
        MySQLComRegisterSlaveCommandPacket packet = new MySQLComRegisterSlaveCommandPacket(
                connectInfo.getServerId(), localAddress.getHostName(), connectInfo.getUsername(), connectInfo.getPassword(), localAddress.getPort());
        channel.writeAndFlush(packet);
        waitExpectedResponse(MySQLOKPacket.class);
    }
    
    private int queryChecksumLength() {
        if (!serverInfo.getServerVersion().greaterThanOrEqualTo(5, 6, 0)) {
            return 0;
        }
        InternalResultSet resultSet = executeQuery("SELECT @@GLOBAL.BINLOG_CHECKSUM");
        String checksumType = resultSet.getFieldValues().get(0).getData().iterator().next().toString();
        switch (checksumType) {
            case "None":
                return 0;
            case "CRC32":
                return 4;
            default:
                throw new UnsupportedOperationException(checksumType);
        }
    }
    
    private void dumpBinlog(final String binlogFileName, final long binlogPosition, final int checksumLength) {
        responseCallback = null;
        channel.pipeline().remove(MySQLCommandPacketDecoder.class);
        channel.pipeline().remove(MySQLCommandResponseHandler.class);
        channel.pipeline().addLast(new MySQLBinlogEventPacketDecoder(checksumLength));
        channel.pipeline().addLast(new MySQLBinlogEventHandler());
        channel.writeAndFlush(new MySQLComBinlogDumpCommandPacket((int) binlogPosition, connectInfo.getServerId(), binlogFileName));
    }
    
    /**
     * Poll binlog event.
     *
     * @return binlog event
     */
    public synchronized AbstractBinlogEvent poll() {
        try {
            return blockingEventQueue.poll(100, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException ignored) {
            return null;
        }
    }
    
    @SuppressWarnings("unchecked")
    private <T> T waitExpectedResponse(final Class<T> type) {
        try {
            Object response = responseCallback.get();
            if (null == response) {
                return null;
            }
            if (type.equals(response.getClass())) {
                return (T) response;
            }
            if (response instanceof MySQLErrPacket) {
                throw new RuntimeException(((MySQLErrPacket) response).getErrorMessage());
            }
            throw new RuntimeException("unexpected response type");
        } catch (final InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private final class MySQLCommandResponseHandler extends ChannelInboundHandlerAdapter {
        
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            if (null != responseCallback) {
                responseCallback.setSuccess(msg);
            }
        }
        
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            if (null != responseCallback) {
                responseCallback.setFailure(cause);
                log.error("protocol resolution error", cause);
            }
        }
    }
    
    private final class MySQLBinlogEventHandler extends ChannelInboundHandlerAdapter {
        
        private AbstractBinlogEvent lastBinlogEvent;
        
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
            if (msg instanceof AbstractBinlogEvent) {
                lastBinlogEvent = (AbstractBinlogEvent) msg;
                blockingEventQueue.put(lastBinlogEvent);
            }
        }
        
        @Override
        public void channelInactive(final ChannelHandlerContext ctx) {
            log.warn("channel inactive");
            reconnect();
        }
        
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            log.error("protocol resolution error", cause);
            reconnect();
        }
        
        private void reconnect() {
            log.info("reconnect mysql client.");
            closeOldChannel();
            connect(eventLoopGroup);
            subscribe(lastBinlogEvent.getFileName(), lastBinlogEvent.getPosition());
        }
        
        private void closeOldChannel() {
            try {
                channel.closeFuture().sync();
            } catch (final InterruptedException ignored) {
            }
        }
    }
}
