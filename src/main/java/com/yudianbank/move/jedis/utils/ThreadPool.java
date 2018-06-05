package com.yudianbank.move.jedis.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadPool {
    private int coreCount;
    private int maxCount;
    private long waitTime;
    private TimeUnit unit;
    private boolean start = true;
    private boolean end;
    private LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    // 我的想法是从list移除来一个Thread对象运行完后再添加进去
    private LinkedBlockingQueue<MyThread> tl = new LinkedBlockingQueue<>();// 用来保存创建自定义的Thread这里最好使用阻塞式队列因为你添加和移除是在不同线程中操作

    public ThreadPool(int coreCount, int maxCount, long waitTime, TimeUnit unit) {
        super();
        this.coreCount = coreCount;
        this.maxCount = maxCount;
        this.waitTime = waitTime;
        this.unit = unit;

        // 创造核心数目线程数量
        for (int i = 0; i < coreCount; i++) {
            tl.add(new MyThread());
        }
    }

    // 改进
    public void execute(Runnable runnable) {
        if (start) {
            runInQueueTask();
            start = false;
        }
        if (tl.size() > 0) {// 如果还有线程空闲就直接执行
            runTask(runnable);
        } else {
            queue.add(runnable);// 添加到任务队列
        }
    }

    private void runInQueueTask() {//这个方法主要是用来开启一个线程来不断扫描任务队列中是否有任务
        new Thread( () -> {
            while (true) {//这里让它不断扫描
                if (end)//如果结受到结束标记就结束
                    break;
                if (queue.size() > 0 && tl.size() > 0) {//如果任务队列不为空并且线程队列也不为空
                    MyThread thread = tl.remove();//从线程阻塞队列中获取一个线程来执行任务
                    thread.setRunnable(queue.remove());
                } else {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        // TODO 自动生成的 catch 块
                        e.printStackTrace();
                    }
                }
            }
        } ).start();
    }

    private void runTask(Runnable runnable) {
        MyThread thread = tl.remove();// 从线程列中获取空余线程

        thread.setRunnable(runnable);// 设置任务到线程上
        if (thread.first)// 如果是第一次执行启动start方法
            thread.start();
    }

    public void shutDown() {//关闭线程池中所有的资源来停止运行
        while (true) {
            if (tl.size() == coreCount&&queue.size()==0) {//只有当任务执行完才关闭它们
                end = true;// 修改标记使扫描任务队列的线程停止
                for (MyThread thread : tl) {// 循环关闭每个线程及让它的start方法执行完
                    thread.closeThread();
                }
                break;
            }else{
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    // TODO 自动生成的 catch 块
                    e.printStackTrace();
                }
            }
        }
    }

    // 改造Thread方法由于调用start方法不能调用多次
    private class MyThread extends Thread {
        private Runnable runnable;
        private Lock lock = new ReentrantLock();
        private Condition condition = lock.newCondition();
        private boolean first = true;
        private boolean shutdown;

        public MyThread() {
        }

        public void setRunnable(Runnable runnable) {//设置任务，到线程中
            lock.lock();
            try {
                this.runnable = runnable;//修改任务
                condition.signal();//唤醒start方法
            } finally {
                lock.unlock();
            }
        }

        public void closeThread() {
            lock.lock();
            try {
                condition.signal();
                shutdown = true;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void run() {
            lock.lock();
            try {
                first = false;
                while (true) {
                    runnable.run();//运行传人的任务
                    tl.add(this);//任务执行完把线程自己回添加线程阻塞队列中方便其它任务用

                    condition.await();//在没有结受到任务时让起阻塞住
                    if (shutdown)
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }

        }
    }

}
