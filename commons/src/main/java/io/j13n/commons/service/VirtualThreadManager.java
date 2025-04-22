package io.j13n.commons.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A service for managing virtual threads in the application.
 * Virtual threads are lightweight threads that are managed by the JVM rather than the OS,
 * allowing for a much larger number of concurrent threads with minimal overhead.
 * 
 * This class provides utilities for creating, submitting, and managing virtual threads.
 * 
 * <h2>Features</h2>
 * <ul>
 *   <li>Create and manage virtual threads</li>
 *   <li>Submit tasks to be executed by virtual threads</li>
 *   <li>Execute collections of tasks concurrently</li>
 *   <li>Schedule tasks to run after a delay</li>
 *   <li>Monitor thread activity and statistics</li>
 *   <li>Gracefully shut down thread execution</li>
 * </ul>
 * 
 * <h2>Usage Examples</h2>
 * 
 * <h3>Basic Usage</h3>
 * <pre>{@code
 * @Service
 * public class MyService {
 *     private final VirtualThreadManager threadManager;
 *     
 *     @Autowired
 *     public MyService(VirtualThreadManager threadManager) {
 *         this.threadManager = threadManager;
 *     }
 *     
 *     public void performTask() {
 *         // Create and start a virtual thread
 *         Thread thread = threadManager.newVirtualThread(() -> {
 *             // Task logic here
 *             System.out.println("Task executed in virtual thread: " + Thread.currentThread().getName());
 *         });
 *         
 *         // The thread is already running
 *     }
 * }
 * }</pre>
 * 
 * <h3>Submitting Tasks</h3>
 * <pre>{@code
 * // Submit a task without a return value
 * Future<Void> future = threadManager.submitTask(() -> {
 *     // Task logic here
 * });
 * 
 * // Submit a task with a return value
 * Future<String> resultFuture = threadManager.submitTask(() -> {
 *     // Task logic here
 *     return "Task result";
 * });
 * 
 * // Get the result (blocks until the task completes)
 * try {
 *     String result = resultFuture.get();
 *     System.out.println("Task result: " + result);
 * } catch (InterruptedException | ExecutionException e) {
 *     // Handle exceptions
 * }
 * }</pre>
 * 
 * <h3>Executing Multiple Tasks</h3>
 * <pre>{@code
 * // Create a collection of tasks
 * List<Callable<String>> tasks = List.of(
 *     () -> "Result 1",
 *     () -> "Result 2",
 *     () -> "Result 3"
 * );
 * 
 * // Execute all tasks and get their results
 * List<String> results = threadManager.executeAllAndGet(tasks);
 * results.forEach(System.out::println);
 * 
 * // Execute all tasks with a timeout
 * Duration timeout = Duration.ofSeconds(5);
 * List<String> timeoutResults = threadManager.executeAllAndGet(tasks, timeout);
 * }</pre>
 * 
 * <h3>Best Practices</h3>
 * <ol>
 *   <li><b>Use virtual threads for I/O-bound tasks</b>: Virtual threads are most beneficial for I/O-bound tasks where threads spend most of their time waiting.</li>
 *   <li><b>Avoid thread-local variables</b>: Virtual threads are not designed to carry thread-local variables for long periods.</li>
 *   <li><b>Monitor thread statistics</b>: Use the monitoring methods to track thread usage and detect potential issues.</li>
 *   <li><b>Graceful shutdown</b>: Always use graceful shutdown in production to ensure tasks are completed properly.</li>
 *   <li><b>Handle exceptions</b>: Always handle exceptions in your tasks to prevent them from being silently swallowed.</li>
 * </ol>
 * 
 * @see java.lang.Thread
 * @see java.util.concurrent.ExecutorService
 * @see java.util.concurrent.Future
 */
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

    /**
     * Submits a task to be executed by a virtual thread.
     *
     * @param task the task to execute
     * @return a Future representing the result of the task
     */
    public Future<Void> submitTask(Runnable task) {
        return virtualThreadExecutor.submit(task, null);
    }

    /**
     * Submits a task that returns a result to be executed by a virtual thread.
     *
     * @param task the task to execute
     * @param <T> the type of the result
     * @return a Future representing the result of the task
     */
    public <T> Future<T> submitTask(Callable<T> task) {
        return virtualThreadExecutor.submit(task);
    }

    /**
     * Submits a task that returns a result to be executed by a virtual thread.
     *
     * @param supplier the supplier of the result
     * @param <T> the type of the result
     * @return a Future representing the result of the task
     */
    public <T> Future<T> submitTask(Supplier<T> supplier) {
        return virtualThreadExecutor.submit(supplier::get);
    }

    /**
     * Executes a collection of tasks using virtual threads and returns a list of Futures.
     *
     * @param tasks the collection of tasks to execute
     * @param <T> the type of the result
     * @return a list of Futures representing the results of the tasks
     */
    public <T> List<Future<T>> executeAll(Collection<Callable<T>> tasks) {
        try {
            return virtualThreadExecutor.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task execution was interrupted", e);
        }
    }

    /**
     * Executes a collection of tasks using virtual threads and returns the result of the first successful task.
     *
     * @param tasks the collection of tasks to execute
     * @param <T> the type of the result
     * @return the result of the first successful task
     * @throws ExecutionException if all tasks fail
     */
    public <T> T executeAny(Collection<Callable<T>> tasks) throws ExecutionException {
        try {
            return virtualThreadExecutor.invokeAny(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task execution was interrupted", e);
        }
    }

    /**
     * Creates a new virtual thread with the given name and task.
     *
     * @param name the name of the thread
     * @param task the task to execute
     * @return the created thread
     */
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

    /**
     * Creates a new virtual thread with the given task.
     *
     * @param task the task to execute
     * @return the created thread
     */
    public Thread newVirtualThread(Runnable task) {
        String name = "virtual-thread-" + totalThreadsCreated.get();
        return newVirtualThread(name, task);
    }

    /**
     * Creates a new virtual thread builder for customizing thread creation.
     *
     * @return a virtual thread builder
     */
    public Thread.Builder.OfVirtual virtualThreadBuilder() {
        return Thread.ofVirtual();
    }

    /**
     * Executes a collection of tasks using virtual threads and waits for all of them to complete.
     * Returns the results of all tasks.
     *
     * @param tasks the collection of tasks to execute
     * @param <T> the type of the result
     * @return a list of results from all tasks
     */
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

    /**
     * Executes a collection of tasks using virtual threads and waits for all of them to complete
     * within the specified timeout. Returns the results of all completed tasks.
     *
     * @param tasks the collection of tasks to execute
     * @param timeout the maximum time to wait
     * @param <T> the type of the result
     * @return a list of results from all completed tasks
     */
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

    /**
     * Schedules a task to run after a delay using a virtual thread.
     *
     * @param task the task to execute
     * @param delay the delay before execution
     * @return a ScheduledFuture representing the scheduled task
     */
    public ScheduledFuture<?> scheduleTask(Runnable task, Duration delay) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        return scheduler.schedule(() -> {
            try {
                virtualThreadExecutor.submit(task);
            } finally {
                scheduler.shutdown();
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Schedules a task that returns a result to run after a delay using a virtual thread.
     *
     * @param task the task to execute
     * @param delay the delay before execution
     * @param <T> the type of the result
     * @return a ScheduledFuture representing the scheduled task
     */
    public <T> ScheduledFuture<T> scheduleTask(Callable<T> task, Duration delay) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<Future<T>> scheduledFuture = scheduler.schedule(() -> {
            try {
                return virtualThreadExecutor.submit(task);
            } finally {
                scheduler.shutdown();
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS);

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
            public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                Future<T> future = scheduledFuture.get(timeout, unit);
                return future.get(timeout, unit);
            }
        };
    }

    /**
     * Gets the current thread statistics.
     *
     * @return a map of thread statistics
     */
    public Map<String, Integer> getThreadStatistics() {
        Map<String, Integer> stats = new ConcurrentHashMap<>();
        stats.put("totalThreadsCreated", totalThreadsCreated.get());
        stats.put("activeThreadCount", activeThreadCount.get());
        stats.put("completedThreadCount", completedThreadCount.get());
        stats.put("failedThreadCount", failedThreadCount.get());
        return stats;
    }

    /**
     * Gets a list of all active thread names.
     *
     * @return a set of active thread names
     */
    public Set<String> getActiveThreadNames() {
        return activeThreads.keySet();
    }

    /**
     * Gets a map of all active threads.
     *
     * @return a map of thread names to thread objects
     */
    public Map<String, Thread> getActiveThreads() {
        return new ConcurrentHashMap<>(activeThreads);
    }

    /**
     * Interrupts a virtual thread by name.
     *
     * @param threadName the name of the thread to interrupt
     * @return true if the thread was found and interrupted, false otherwise
     */
    public boolean interruptThread(String threadName) {
        Thread thread = activeThreads.get(threadName);
        if (thread != null) {
            thread.interrupt();
            return true;
        }
        return false;
    }

    /**
     * Interrupts all active virtual threads.
     */
    public void interruptAllThreads() {
        activeThreads.values().forEach(Thread::interrupt);
    }

    /**
     * Initiates an orderly shutdown of the virtual thread executor.
     * Previously submitted tasks will be executed, but no new tasks will be accepted.
     */
    public void shutdown() {
        virtualThreadExecutor.shutdown();
        logger.info("Virtual thread executor shutdown initiated");
    }

    /**
     * Attempts to stop all actively executing tasks and halts the processing of waiting tasks.
     *
     * @return a list of tasks that were awaiting execution
     */
    public List<Runnable> shutdownNow() {
        List<Runnable> pendingTasks = virtualThreadExecutor.shutdownNow();
        logger.info("Virtual thread executor shutdown forced, {} pending tasks cancelled", pendingTasks.size());
        return pendingTasks;
    }

    /**
     * Blocks until all tasks have completed execution after a shutdown request,
     * or the timeout occurs, or the current thread is interrupted, whichever happens first.
     *
     * @param timeout the maximum time to wait
     * @return true if the executor terminated, false if the timeout elapsed before termination
     */
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

    /**
     * Checks if the virtual thread executor has been shut down.
     *
     * @return true if the executor has been shut down, false otherwise
     */
    public boolean isShutdown() {
        return virtualThreadExecutor.isShutdown();
    }

    /**
     * Checks if the virtual thread executor has been terminated.
     * If the executor has been terminated, all tasks have completed following shut down.
     *
     * @return true if the executor has been terminated, false otherwise
     */
    public boolean isTerminated() {
        return virtualThreadExecutor.isTerminated();
    }

    /**
     * Performs a graceful shutdown of the virtual thread executor.
     * This method:
     * 1. Initiates an orderly shutdown
     * 2. Waits for the specified timeout for tasks to complete
     * 3. Forces shutdown if tasks don't complete within the timeout
     *
     * @param timeout the maximum time to wait for tasks to complete
     * @return true if the executor terminated gracefully, false if it was forced to terminate
     */
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
