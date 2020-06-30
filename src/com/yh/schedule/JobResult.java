package com.yh.schedule;

/**
 * @ClassName JobResult
 * @Description 任务执行结果
 * @Author yh
 * @Date 2020-06-18 14:14
 * @Version 1.0
 */
public class JobResult {

    private JobResultEnum jobResult = JobResultEnum.FAIL;

    public JobResultEnum getJobResult() {
        return jobResult;
    }

    public void setJobResult(JobResultEnum jobResult) {
        this.jobResult = jobResult;
    }

    public JobResult success() {
        this.jobResult = JobResultEnum.SUCCESS;
        return this;
    }

    public JobResult fail() {
        this.jobResult = JobResultEnum.FAIL;
        return this;
    }
}
