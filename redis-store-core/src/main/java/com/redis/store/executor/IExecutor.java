package com.redis.store.executor;

/**
 * Author LTY
 * Date 2018/05/23
 */
public interface IExecutor<T> {
    boolean execute(T t);
}
