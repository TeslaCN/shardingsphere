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
import io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueue;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Experimental.
 * Using Epoll instead of {@link java.util.concurrent.ThreadPoolExecutor} with {@link java.util.concurrent.LinkedBlockingQueue} may have higher throughput in some situation.
 */
@Slf4j
public final class EpollSingleThreadExecutorService implements ExecutorService, Runnable {
    
    private final Queue<Runnable> workQueue = new MpscArrayQueue<>(128);
    
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
    
    /**
     * Is {@link EpollSingleThreadExecutorService} available.
     *
     * @return is {@link EpollSingleThreadExecutorService} available
     */
    public static boolean isAvailable() {
        return Epoll.isAvailable();
    }
    
    @Override
    public void run() {
        while (!shutdown) {
            Runnable work = workQueue.poll();
            if (null == work) {
                try {
                    Native.epollWait(epollFd, events, false);
                } catch (final Throwable t) {
                    log.warn("Epoll wait error in " + workThread.getName(), t);
                }
            } else {
                try {
                    work.run();
                } catch (final Throwable t) {
                    log.error("Error occurred in " + workThread.getName(), t);
                }
            }
        }
        synchronized (this) {
            notifyAll();
        }
    }
    
    @Override
    public void execute(final Runnable command) {
        workQueue.add(command);
        Native.eventFdWrite(eventFd.intValue(), 1L);
    }
    
    @Override
    public void shutdown() {
        shutdown = true;
        Native.eventFdWrite(eventFd.intValue(), 1L);
        workThread.interrupt();
        closeResources();
    }
    
    private void closeResources() {
        try {
            try {
                eventFd.close();
            } catch (IOException e) {
                log.warn("Failed to close the event fd.", e);
            }
            try {
                epollFd.close();
            } catch (IOException e) {
                log.warn("Failed to close the epoll fd.", e);
            }
        } finally {
            events.free();
        }
    }
    
    @Override
    public boolean isShutdown() {
        return shutdown;
    }
    
    @Override
    public boolean isTerminated() {
        return !workThread.isAlive();
    }
    
    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        synchronized (this) {
            wait(unit.toMillis(timeout));
        }
        return !workThread.isAlive();
    }
    
    @Override
    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException();
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
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public <T> T invokeAny(final Collection<? extends Callable<T>> tasks, final long timeout, final TimeUnit unit) {
        throw new UnsupportedOperationException();
    }
}
