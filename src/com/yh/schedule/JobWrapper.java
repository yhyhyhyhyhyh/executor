package com.yh.schedule;

import com.yh.ScheduleException;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName JobWrapper
 * @Description 任务执行类
 * @Author yh
 * @Date 2020-06-17 9:42
 * @Version 1.0
 */
public class JobWrapper<T,V extends JobResult> {

    private String id;

    private T param;

    private Job<T,V> job;

    private JobCallBack callBack;

    private Set<JobWrapper> dependencies = new HashSet<>();

    private Set<JobWrapper> nextJobs = new HashSet<>();

    private volatile AtomicInteger jobState = new AtomicInteger(JobState.READY.getCode());

    private JobGroup group;

    void doJob(Executor pool) {
        //根据state判断当前job是否已经执行，若已经被调度,直接返回
        if(!JobState.READY.getCode().equals(jobState.get())) {
            return;
        }
        // 若当前任务执行失败，直接返回，递归时依赖当前job的所有job均为失败
        // 若当前任务正在运行,直接返回，该任务执行完成后主动通知下级任务开始运行
        // 依赖检测，优先执行依赖的Job
        if(hasDependenciesComplete()) {
            //当依赖的job全都执行完毕，执行当前job
            //cas保证job只会被调度一次
            if(jobState.compareAndSet(JobState.READY.getCode(),JobState.RUNNING.getCode())) {
                try {
                    V result = executeThis();
                    if(result.getJobResult() == JobResultEnum.SUCCESS) {
                        //执行成功，将当前job状态变更为完成，将结果加入group
                        group.setResult(id,result);
                        jobState.set(JobState.COMPLETE.getCode());
                        //若当前job设置了回调，调用回调
                        if(callBack != null) {
                            //若是异步回调，将回调函数加入jobGroup的线程池执行
                            if(callBack instanceof AsyncJobCallBack) {
                                CompletableFuture.runAsync(()-> callBack.callBack(param,result),pool);
                            } else {
                                callBack.callBack(param,result);
                            }
                        }
                    } else {
                        //执行失败，当前job设置为FAIL，并将下级任务快速失败
                        jobState.set(JobState.FAIL.getCode());
                        group.setResult(id,new JobResult().fail());
                        failFastNext();
                        return;
                    }
                    executeNext(pool);
                } catch (Exception e) {
                    e.printStackTrace();
                    jobState.set(JobState.FAIL.getCode());
                    group.setResult(id,new JobResult().fail());
                    throw new ScheduleException(e);
                }
                this.jobState.set(JobState.COMPLETE.getCode());
            }
        } else {
            return;
        }
    }

    /**
     * 当前job依赖的job是否全部执行完成
     * @return
     */
    boolean hasDependenciesComplete() {
        if(dependencies == null || dependencies.size() == 0) {
            return true;
        }
        for(JobWrapper depend : dependencies) {
            if( JobState.COMPLETE.getCode().equals(depend.jobState.get())) {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    V executeThis() throws InterruptedException {
        return job.run(param);
    }

    void failFastNext() {
        if(nextJobs == null || nextJobs.size() == 0) {
            return;
        }
        for(JobWrapper job : nextJobs) {
            //通过cas进行快速失败操作
            if(job.jobState.compareAndSet(JobState.READY.getCode(),JobState.FAIL.getCode())) {
                group.setResult(job.id,new JobResult().fail());
                job.failFastNext();
            }
        }
    }

    void executeNext(Executor pool) {
        //当前job已执行，对下一级任务进行调度
        if( nextJobs == null || nextJobs.size() == 0) {
            return;
        }
        for(JobWrapper jobWrapper : nextJobs) {
            CompletableFuture.runAsync(()->jobWrapper.doJob(pool));
        }
    }

    public String getId() {
        return id;
    }

    boolean hasDependency() {
        return dependencies.size()==0?false:true;
    }

    /**
     * 是否存在环
     * @param target
     * @param path
     * @param direct
     * @return
     */
    private boolean hasLoopInternal(JobWrapper target,JobWrapper path,int direct) {
        if( direct == 1) {
            if(path.nextJobs.size()>0) {
                if(path.nextJobs.contains(target)) {
                    return true;
                }
                for( Object next : path.nextJobs ) {
                    return hasLoopInternal(target,(JobWrapper) next,1);
                }
            }
        }
        if( direct == 2) {
            if(dependencies.size()>0) {
                if(dependencies.contains(target)) {
                    return true;
                }
                for(Object depend : path.dependencies) {
                    return hasLoopInternal(target, (JobWrapper) depend, 2);
                }
            }
        }
        return false;
    }

    boolean hasLoop() {
        return hasLoopInternal(this,this,1)||hasLoopInternal(this,this,2);
    }

    static class Builder<T,V extends JobResult> {

        private String id;

        private T param;

        private Job<T,V> job;

        private JobCallBack callBack;

        private Set<JobWrapper> dependencies = new HashSet<>();

        private Set<JobWrapper> nextJobs = new HashSet<>();

        private JobGroup jobGroup;

        Builder<T,V> id(String id) {
            this.id = id;
            return this;
        }

        Builder<T,V> param(T param) {
            this.param = param;
            return this;
        }

        Builder<T,V> job(Job<T,V> job) {
            this.job = job;
            return this;
        }

        Builder<T,V> callBack(JobCallBack<T,V> callBack) {
            this.callBack = callBack;
            return this;
        }

        Builder<T,V> next(JobWrapper jobWrapper) {
            if(Objects.isNull(jobWrapper)) {
                return this;
            }
            nextJobs.add(jobWrapper);
            return this;
        }

        Builder<T,V> depend(JobWrapper jobWrapper) {
            if(Objects.isNull(jobWrapper)) {
                return this;
            }
            dependencies.add(jobWrapper);
            return this;
        }

        Builder<T,V> jobGroup(JobGroup jobGroup) {
            this.jobGroup = jobGroup;
            return this;
        }

        public JobWrapper build() {
            if(jobGroup == null) {
                throw new ScheduleException("job未指定jobGroup");
            }
            if( id == null || jobGroup.hasIdExists(id)) {
                throw new ScheduleException("id不可重复且不可为空");
            }
            JobWrapper jobWrapper = new JobWrapper();
            jobWrapper.nextJobs = nextJobs;
            jobWrapper.dependencies = dependencies;
            jobWrapper.param = param;
            jobWrapper.callBack = callBack;
            jobWrapper.id = id;
            jobWrapper.job = job;
            jobWrapper.group = jobGroup;
            for(JobWrapper nextJob : nextJobs) {
                nextJob.dependencies.add(jobWrapper);
            }
            for(JobWrapper depend : dependencies) {
                depend.nextJobs.add(jobWrapper);
            }
            if(jobWrapper.hasLoop()) {
                throw new ScheduleException("id:"+jobWrapper.getId()+"存在环");
            }
            jobGroup.addJob(jobWrapper);
            return jobWrapper;
        }

    }


}
