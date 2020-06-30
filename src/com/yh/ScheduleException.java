package com.yh;

/**
 * @ClassName ScheduleException
 * @Description TODO
 * @Author yh
 * @Date 2020-06-17 16:11
 * @Version 1.0
 */
public class ScheduleException extends RuntimeException {

    public ScheduleException(String message) {
        super(message);
    }

    public ScheduleException(Throwable throwable) {
        super(throwable);
    }
}
