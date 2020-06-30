package com.yh.schedule;

/**
 * @ClassName Job
 * @Description TODO
 * @Author yh
 * @Date 2020-06-17 10:43
 * @Version 1.0
 */
@FunctionalInterface
public interface Job<T,V extends JobResult> {

    V run(T param) throws InterruptedException;

}
