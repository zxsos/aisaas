package io.bangbang.commons.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Service
public class VirtualThreadManager {

    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadManager.class);

    private final ExecutorService virtualThreadExecutor;
    private final ConcurrentHashMap<String, Thread> activeThreads = new ConcurrentHashMap<>();
    private final AtomicInteger totalThreadsCreated = new AtomicInteger(0);
    private final AtomicInteger activeThreadCount = new AtomicInteger(0);
    private final AtomicInteger completedThreadCount = new AtomicInteger(0);
    private final AtomicInteger failedThreadCount = new AtomicInteger(0);

    public VirtualThreadManager() {
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        logger.info("VirtualThreadManager initialized with virtual thread executor");
    }

    public Future<Void> submitTask(Runnable task) {
        return virtualThreadExecutor.submit(task, null);
    }

    public <T> Future<T> submitTask(Callable<T> task) {
        return virtualThreadExecutor.submit(task);
    }

    public <T> Future<T> submitTask(Supplier<T> supplier) {
        return virtualThreadExecutor.submit(supplier::get);
    }

    public <T> List<Future<T>> executeAll(Collection<Callable<T>> tasks) {
        try {
            return virtualThreadExecutor.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task execution was interrupted", e);
        }
    }

    public <T> T executeAny(Collection<Callable<T>> tasks) throws ExecutionException {
        try {
            return virtualThreadExecutor.invokeAny(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task execution was interrupted", e);
        }
    }

    public Thread newVirtualThread(String name, Runnable task) {
        totalThreadsCreated.incrementAndGet();
        activeThreadCount.incrementAndGet();

        Runnable wrappedTask = () -> {
            try {
                task.run();
                completedThreadCount.incrementAndGet();
            } catch (Exception e) {
                failedThreadCount.incrementAndGet();
                logger.error("Virtual thread execution failed: {}", e.getMessage(), e);
                throw e;
            } finally {
                activeThreadCount.decrementAndGet();
                activeThreads.remove(name);
            }
        };

        Thread thread = Thread.ofVirtual().name(name).start(wrappedTask);
        activeThreads.put(name, thread);
        return thread;
    }

    public Thread newVirtualThread(Runnable task) {
        String name = "virtual-thread-" + totalThreadsCreated.get();
        return newVirtualThread(name, task);
    }

    public Thread.Builder.OfVirtual virtualThreadBuilder() {
        return Thread.ofVirtual();
    }

    public <T> List<T> executeAllAndGet(Collection<Callable<T>> tasks) {
        List<Future<T>> futures = executeAll(tasks);
        return futures.stream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Task execution was interrupted", e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException("Task execution failed", e);
                    }
                })
                .collect(Collectors.toList());
    }

    public <T> List<T> executeAllAndGet(Collection<Callable<T>> tasks, Duration timeout) {
        try {
            List<Future<T>> futures = virtualThreadExecutor.invokeAll(tasks, timeout.toMillis(), TimeUnit.MILLISECONDS);
            return futures.stream()
                    .filter(Future::isDone)
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Task execution was interrupted", e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException("Task execution failed", e);
                        }
                    })
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task execution was interrupted", e);
        }
    }

    public ScheduledFuture<?> scheduleTask(Runnable task, Duration delay) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        return scheduler.schedule(
                () -> {
                    try {
                        virtualThreadExecutor.submit(task);
                    } finally {
                        scheduler.shutdown();
                    }
                },
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public <T> ScheduledFuture<T> scheduleTask(Callable<T> task, Duration delay) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<Future<T>> scheduledFuture = scheduler.schedule(
                () -> {
                    try {
                        return virtualThreadExecutor.submit(task);
                    } finally {
                        scheduler.shutdown();
                    }
                },
                delay.toMillis(),
                TimeUnit.MILLISECONDS);

        return new ScheduledFuture<T>() {
            @Override
            public long getDelay(TimeUnit unit) {
                return scheduledFuture.getDelay(unit);
            }

            @Override
            public int compareTo(Delayed o) {
                return scheduledFuture.compareTo(o);
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return scheduledFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return scheduledFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return scheduledFuture.isDone();
            }

            @Override
            public T get() throws InterruptedException, ExecutionException {
                return scheduledFuture.get().get();
            }

            @Override
            public T get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                Future<T> future = scheduledFuture.get(timeout, unit);
                return future.get(timeout, unit);
            }
        };
    }

    public Map<String, Integer> getThreadStatistics() {
        Map<String, Integer> stats = new ConcurrentHashMap<>();
        stats.put("totalThreadsCreated", totalThreadsCreated.get());
        stats.put("activeThreadCount", activeThreadCount.get());
        stats.put("completedThreadCount", completedThreadCount.get());
        stats.put("failedThreadCount", failedThreadCount.get());
        return stats;
    }

    public Set<String> getActiveThreadNames() {
        return activeThreads.keySet();
    }

    public Map<String, Thread> getActiveThreads() {
        return new ConcurrentHashMap<>(activeThreads);
    }

    public boolean interruptThread(String threadName) {
        Thread thread = activeThreads.get(threadName);
        if (thread != null) {
            thread.interrupt();
            return true;
        }
        return false;
    }

    public void interruptAllThreads() {
        activeThreads.values().forEach(Thread::interrupt);
    }

    public void shutdown() {
        virtualThreadExecutor.shutdown();
        logger.info("Virtual thread executor shutdown initiated");
    }

    public List<Runnable> shutdownNow() {
        List<Runnable> pendingTasks = virtualThreadExecutor.shutdownNow();
        logger.info("Virtual thread executor shutdown forced, {} pending tasks cancelled", pendingTasks.size());
        return pendingTasks;
    }

    public boolean awaitTermination(Duration timeout) {
        try {
            boolean terminated = virtualThreadExecutor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (terminated) {
                logger.info("Virtual thread executor terminated successfully");
            } else {
                logger.warn("Virtual thread executor did not terminate within the specified timeout");
            }
            return terminated;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Awaiting termination of virtual thread executor was interrupted");
            return false;
        }
    }

    public boolean isShutdown() {
        return virtualThreadExecutor.isShutdown();
    }

    public boolean isTerminated() {
        return virtualThreadExecutor.isTerminated();
    }

    public boolean gracefulShutdown(Duration timeout) {
        shutdown();
        boolean terminated = false;
        try {
            terminated = virtualThreadExecutor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!terminated) {
                List<Runnable> pendingTasks = shutdownNow();
                logger.warn("Graceful shutdown timed out, forced shutdown with {} pending tasks", pendingTasks.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            shutdownNow();
            logger.warn("Graceful shutdown was interrupted, forced shutdown initiated");
        }
        return terminated;
    }
}
