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

package org.apache.shardingsphere.proxy.frontend;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.db.protocol.GlobalContext;
import org.apache.shardingsphere.proxy.backend.context.BackendExecutorContext;
import org.apache.shardingsphere.proxy.frontend.netty.ServerHandlerInitializer;
import org.apache.shardingsphere.proxy.frontend.protocol.FrontDatabaseProtocolTypeFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;

/**
 * ShardingSphere-Proxy.
 */
@Slf4j
public final class ShardingSphereProxy {
    
    private EventLoopGroup bossGroup;
    
    private EventLoopGroup workerGroup;
    
    /**
     * Start ShardingSphere-Proxy.
     *
     * @param port port
     */
    @SneakyThrows(InterruptedException.class)
    public void start(final int port) {
        try {
            createEventLoopGroup();
            ServerBootstrap bootstrap = new ServerBootstrap();
            initServerBootstrap(bootstrap);
            ChannelFuture future = bootstrap.bind(port).sync();
            log.info("ShardingSphere-Proxy start success.");
            doExecute();
            future.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            BackendExecutorContext.getInstance().getExecutorEngine().close();
        }
    }
    
    private void createEventLoopGroup() {
        bossGroup = Epoll.isAvailable() ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
        workerGroup = Epoll.isAvailable() ? new EpollEventLoopGroup() : new NioEventLoopGroup();
    }
    
    private void initServerBootstrap(final ServerBootstrap bootstrap) {
        bootstrap.group(bossGroup, workerGroup)
                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8 * 1024 * 1024, 16 * 1024 * 1024))
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ServerHandlerInitializer(FrontDatabaseProtocolTypeFactory.getDatabaseType()));
    }
    
    private void doExecute() {
        //        String url = "jdbc:mysql://localhost:3306/demo_ds_0?useServerPrepStmts=true&cachePrepStmts=true&useSSL=false";
//        String sql = "SELECT * FROM t_order_0 WHERE order_id=?";
        String url = "jdbc:mysql://127.0.0.1:13306/sharding_db?useSSL=false&useServerPrepStmts=true&cachePrepStmts=true";
        String sql = "SELECT * FROM t_order WHERE order_id=? and user_id = ?";
        String username = "root";
        String password = "sudo reboot";
        int times = 1000;
        long[] took = new long[times];
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                for (int i = 0; i < times; i++) {
                    took[i] = executeQuery(preparedStatement);
                }
            }
        } catch (final SQLException ex) {
            ex.printStackTrace();
        }
        Arrays.sort(took);
        long total = 0;
        for (int i = 1; i < took.length - 1; i++) {
            total += took[i];
        }
        System.out.println(MessageFormat.format("Average: {0} us", total / (took.length - 2)));
    }
    
    private long executeQuery(final PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setLong(1, 1);
        preparedStatement.setLong(2, 1);
        long before = GlobalContext.clientStart = System.nanoTime() / 1000;
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                resultSet.getObject(1);
            }
        }
        long time = System.nanoTime() / 1000 - before;
        System.out.println(MessageFormat.format("{0} us", time));
        return time;
    }
}
