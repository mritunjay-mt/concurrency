package org.mt.concurrency;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SchedulerService {

    Scheduler scheduler;

    public SchedulerService() {
        this.scheduler = new Scheduler();
    }

    public void scheduleWithInitialDelay(Runnable task, long initialDelay) {
        InitialDelayTaskWrapper wrapper = new InitialDelayTaskWrapper(
                task,
                0,
                System.currentTimeMillis() + initialDelay
        );
        scheduler.add(wrapper);
    }

    public void scheduleWithFixedInterval(Runnable task, long initialDelay, long interval) {
        FixedIntervalTaskWrapper wrapper = new FixedIntervalTaskWrapper(
                task,
                interval,
                System.currentTimeMillis() + initialDelay + interval
        );
        scheduler.add(wrapper);
    }


}

class Scheduler {

    Queue<TaskWrapper> queue;
    ReentrantLock lock;
    Condition newTaskAvailable;

    Scheduler() {
        Comparator<TaskWrapper> cmp = Comparator.comparingLong(TaskWrapper::getExecutionTime);
        this.queue = new PriorityQueue<>(cmp);
        this.lock = new ReentrantLock();
        this.newTaskAvailable = lock.newCondition();

        (new Thread(() -> {
            try {
                schedule();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        })).start();
    }

    void schedule() throws InterruptedException {
        while (true) {
            lock.lock();
            try {
                long sleep = getSleepTimeUntilNextTask();
                while (sleep > 0) {
                    newTaskAvailable.await(sleep, TimeUnit.MILLISECONDS);
                    sleep = getSleepTimeUntilNextTask();
                }

                TaskWrapper latestTask = queue.remove();
                (new Thread(() -> execute(latestTask))).start();
            } finally {
                lock.unlock();
            }
        }
    }

    void execute(TaskWrapper latestTask) {
        latestTask.run();

        if (latestTask.shouldSchedule()) {
            lock.lock();
            try {
                queue.add(latestTask.getNextTaskInstance());
                newTaskAvailable.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    long getSleepTimeUntilNextTask() {
        return queue.isEmpty()
                ? Long.MAX_VALUE
                : queue.peek().getExecutionTime() - System.currentTimeMillis();
    }


    public void add(TaskWrapper wrapper) {
        lock.lock();
        try {
            queue.add(wrapper);
            newTaskAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }
}

interface TaskWrapper {
    long getExecutionTime();
    void run();

    boolean shouldSchedule();

    TaskWrapper getNextTaskInstance();
}

class InitialDelayTaskWrapper implements TaskWrapper {
    Runnable task;
    long interval;
    long executionTime;

    InitialDelayTaskWrapper(Runnable task, long interval, long executionTime) {
        this.task = task;
        this.interval = interval;
        this.executionTime = executionTime;
    }

    @Override
    public long getExecutionTime() {
        return executionTime;
    }

    @Override
    public void run() {
        task.run();
    }

    @Override
    public boolean shouldSchedule() {
        return false;
    }

    @Override
    public TaskWrapper getNextTaskInstance() {
        throw new RuntimeException("Not Supported");
    }
}

// Fixed interval = task = 2sec, inter = 5 => 0, 5, 10, 15
// Fixed delay = task = 2, delay = 5, = 0, 7, 14
class FixedIntervalTaskWrapper implements TaskWrapper {
    Runnable task;
    long interval;
    long executionTime;

    FixedIntervalTaskWrapper(Runnable task, long interval, long executionTime) {
        this.task = task;
        this.interval = interval;
        this.executionTime = executionTime;
    }

    @Override
    public long getExecutionTime() {
        return executionTime;
    }

    @Override
    public void run() {
        task.run();
    }

    @Override
    public boolean shouldSchedule() {
        return true;
    }

    @Override
    public TaskWrapper getNextTaskInstance() {
        return new FixedIntervalTaskWrapper(
                task,
                interval,
                executionTime + interval
        );
    }
}
