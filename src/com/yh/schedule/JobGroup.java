package com.yh.schedule;

import com.yh.ScheduleException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @ClassName JobGroup
 * @Description 任务组，同一个任务组共享一个线程池资源
 * @Author yh
 * @Date 2020-06-17 13:16
 * @Version 1.0
 */
public class JobGroup {

    private final Executor DEFAULT_POOL = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private Executor providPool;

    private Map<String,Object> jobGroupResultSet = new ConcurrentHashMap<String,Object>();

    private Map<String,JobWrapper> jobMap = new ConcurrentHashMap<>();

    private ReentrantLock lock = new ReentrantLock();

    private Condition condition = lock.newCondition();

    private GroupCallBack callBack;

    void addJob(JobWrapper jobWrapper) {
        jobMap.put(jobWrapper.getId(),jobWrapper);
    }

    boolean hasIdExists(String id) {
        if( jobMap.get(id)!=null) {
            return true;
        } else {
            return false;
        }
    }

    void setResult(String id, Object result) {
        jobGroupResultSet.put(id,result);
        if(jobGroupResultSet.size() == jobMap.size()) {
            //所有job执行完毕
            try {
                lock.lock();
                condition.signal();
            }finally {
                lock.unlock();
            }
        }
    }

    /**
     * 执行jobGroup
     */
    void execute() {
        // 遍历出所有没有依赖的job 并行调度
        List<JobWrapper> noneDependJobs = jobMap.values().stream().filter( job -> !job.hasDependency()).collect(Collectors.toList());
        if( noneDependJobs == null || noneDependJobs.size() == 0) {
            throw new ScheduleException("不存在可执行的起点");
        }
        Executor pool = providPool == null?DEFAULT_POOL:providPool;
        for(int i = 0,len = noneDependJobs.size();i<len;i++) {
            JobWrapper noneDependJob = noneDependJobs.get(i);
            CompletableFuture.runAsync(()-> noneDependJob.doJob(pool),pool);
        }
        //等待所有job执行完成
        lock.lock();
        try {
            condition.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        if(callBack != null) {
            //若传入了回调函数，所有job调度完成之后调用回调函数
            if( callBack instanceof AsyncJobCallBack) {
                CompletableFuture.runAsync(()-> callBack.callback(this));
            } else {
                callBack.callback(this);
            }
        }
        if( pool instanceof ExecutorService) {
            ((ExecutorService) pool).shutdown();
        }
    }




    private JobGroup() {

    }

    static class Builder {

        private Executor providPool;

        private GroupCallBack callBack;

        public Builder executor(Executor providPool) {
            this.providPool = providPool;
            return this;
        }

        public Builder callback(GroupCallBack callBack) {
            this.callBack = callBack;
            return this;
        }

        public JobGroup build() {
            JobGroup group = new JobGroup();
            group.providPool = providPool;
            group.callBack = callBack;
            return group;
        }
    }

    Map<String,Object> getJobGroupResultSet() {
        return jobGroupResultSet;
    }
}
