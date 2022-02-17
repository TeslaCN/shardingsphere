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

package io.netty.channel.epoll;

import io.netty.channel.unix.FileDescriptor;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class EpollSingleThreadExecutorService implements ExecutorService, Runnable {
    
    private final Queue<Runnable> workQueue = new MpscUnboundedArrayQueue<>(128);
    
    private final EpollEventArray events = new EpollEventArray(16);
    
    private final FileDescriptor epollFd;
    
    private final FileDescriptor eventFd;
    
    private final Thread workThread;
    
    private volatile boolean shutdown;
    
    public EpollSingleThreadExecutorService(final String threadName) {
        boolean success = false;
        FileDescriptor epollFd = null;
        FileDescriptor eventFd = null;
        try {
            this.epollFd = epollFd = Native.newEpollCreate();
            this.eventFd = eventFd = Native.newEventFd();
            try {
                Native.epollCtlAdd(epollFd.intValue(), eventFd.intValue(), Native.EPOLLIN | Native.EPOLLET);
            } catch (final IOException ex) {
                throw new IllegalStateException("Unable to add eventFd file descriptor to epoll", ex);
            }
            success = true;
        } finally {
            if (!success) {
                if (epollFd != null) {
                    try {
                        epollFd.close();
                    } catch (final IOException ignored) {
                    }
                }
                if (eventFd != null) {
                    try {
                        eventFd.close();
                    } catch (final IOException ignored) {
                    }
                }
            }
        }
        workThread = new Thread(this, threadName);
        workThread.start();
    }
    
    @Override
    public void run() {
        while (!shutdown) {
            Runnable work = workQueue.poll();
            if (null == work) {
                try {
                    epollWaitNoTimerChange();
                } catch (final IOException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                work.run();
            }
        }
    }
    
    private void epollWaitNoTimerChange() throws IOException {
        Native.epollWait(epollFd, events, false);
    }
    
    @Override
    public void execute(final Runnable command) {
        workQueue.add(command);
        Native.eventFdWrite(eventFd.intValue(), 1L);
    }
    
    @Override
    public void shutdown() {
        shutdown = true;
        workThread.interrupt();
    }
    
    @Override
    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isShutdown() {
        return shutdown;
    }
    
    @Override
    public boolean isTerminated() {
        return false;
    }
    
    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        return false;
    }
    
    @Override
    public <T> Future<T> submit(final Callable<T> task) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <T> Future<T> submit(final Runnable task, final T result) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Future<?> submit(final Runnable task) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }
}
