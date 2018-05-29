package com.redis.store.util;

import org.I0Itec.zkclient.serialize.ZkSerializer;


public class ZkStringSerializer implements ZkSerializer {

    public static final ZkStringSerializer INSTANCE = new ZkStringSerializer();

    private ZkStringSerializer() {
    }

    @Override
    public byte[] serialize(Object data) {
        return data == null ? new byte[0] : data.toString().getBytes();
    }

    @Override
    public String deserialize(byte[] bytes) {
        return bytes == null ? null : new String(bytes);
    }
}
