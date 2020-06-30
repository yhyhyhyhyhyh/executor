package com.yh.schedule;

/**
 * @ClassName JobCallBack
 * @Description 回调函数
 * @Author yh
 * @Date 2020-06-17 10:44
 * @Version 1.0
 */
public interface JobCallBack<T,S>{

    void callBack(T param,S workResult);

}
