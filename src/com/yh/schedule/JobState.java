package com.yh.schedule;

/**
 * @ClassName JobState
 * @Description job状态枚举
 * @Author yh
 * @Date 2020-06-17 13:09
 * @Version 1.0
 */
public enum  JobState {
    READY(0),
    RUNNING(1),
    COMPLETE(2),
    FAIL(3);

    private Integer code;

    JobState(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return this.code;
    }
}
