package com.redis.store.util;

import java.util.Arrays;
import java.util.List;

public class CommonUtils {

    public static String str(List<?> list) {
        return list == null ? "[]" : Arrays.toString(list.toArray());
    }

    /**
     * 从path获取name(去掉首斜杠)
     */
    public static String stripName(String path) {
        return path.substring(path.indexOf("/") + 1);
    }
}
