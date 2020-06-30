package com.yh.schedule;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @ClassName Scheduler
 * @Description 调度器
 * @Author yh
 * @Date 2020-06-17 18:22
 * @Version 1.0
 */
public class Scheduler {

    private Scheduler() {

    }

    /**
     * 提交jobGroup
     * @param jobGroup
     */
    public static void submitJobGroup(JobGroup jobGroup) {
        long start = System.currentTimeMillis();
        jobGroup.execute();
        long end = System.currentTimeMillis();
        System.out.println( String.format("总耗时:%s ms",String.valueOf(end - start)));
    }

    public static void main(String[] args) {
        Executor pool = Executors.newFixedThreadPool(10);
        JobGroup group  = new JobGroup.Builder()
                .executor(pool)
                .callback( jobGroup -> System.out.println("jobGroup finish,result is:"+jobGroup.getJobGroupResultSet()))
                .build();
        JobWrapper<String,JobResult> job1 = new JobWrapper.Builder<String,JobResult>()
                .id("1")
                .jobGroup(group)
                .job( str -> {
                    System.out.println(" job 1 execute,param"+str);
                    Thread.sleep(1000L);
                    return new JobResult().success();
                })
                .callBack((param,result)-> {
                    System.out.println("job 1 finished");
                })
                .param("1")
                .build();

        JobWrapper<String,JobResult> job2 = new JobWrapper.Builder<String,JobResult>()
                .id("2")
                .jobGroup(group)
                .job( str -> {
                    System.out.println(" job 2 execute,param"+str);
                    Thread.sleep(1000L);
                    return new JobResult().success();
                })
                .callBack((param,result)-> {
                    System.out.println("job 2 finished");
                })
                .param("2")
                .depend(job1)
                .build();

        JobWrapper<String,JobResult> job3 = new JobWrapper.Builder<String,JobResult>()
                .id("3")
                .jobGroup(group)
                .job( str -> {
                    System.out.println(" job 3 execute,param"+str);
                    Thread.sleep(1000L);
                    return new JobResult().fail();
                })
                .callBack((param,result)-> {
                    System.out.println("job 3 finished");
                })
                .param("3")
                .depend(job1)
                .build();

        JobWrapper<String,JobResult> job4 = new JobWrapper.Builder<String,JobResult>()
                .id("4")
                .jobGroup(group)
                .job( str -> {
                    System.out.println(" job 4 execute,param"+str);
                    Thread.sleep(1000L);
                    return new JobResult().success();
                })
                .callBack((param,result)-> {
                    System.out.println("job 4 finished");
                })
                .param("4")
                .depend(job2)
                .depend(job3)
                .build();

        Scheduler.submitJobGroup(group);
    }
}
