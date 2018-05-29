package com.redis.store.util;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.TypeReference;

import java.util.List;
import java.util.Map;

/**
 * Author LTY
 * Date 2018/05/23
 */
public class JsonUtils {

    private static final Logger LOGGER = Logger.getLogger(JsonUtils.class);

    private static ThreadLocal<ObjectMapper> objMapperLocal = ThreadLocal.withInitial(() -> (new ObjectMapper()).configure(JsonParser.Feature.INTERN_FIELD_NAMES, false));

    private JsonUtils() {
    }

    public static String toJSON(Object value) {
        String result = null;

        try {
            result = objMapperLocal.get().writeValueAsString(value);
        } catch (Exception var3) {
            LOGGER.error(var3);
        }

        if ("null".equals(result)) {
            result = null;
        }

        return result;
    }

    public static <T> T toT(String jsonString, Class<T> clazz) {
        try {
            return objMapperLocal.get().readValue(jsonString, clazz);
        } catch (Exception var3) {
            LOGGER.error("toT error: " + jsonString, var3);
            return null;
        }
    }

    public static <T> T toT(String jsonString, TypeReference valueTypeRef) {
        try {
            return ((ObjectMapper) objMapperLocal.get()).readValue(jsonString, valueTypeRef);
        } catch (Exception var3) {
            LOGGER.error("toT error: " + jsonString, var3);
            return null;
        }
    }

    public static <T> List<T> toTList(String jsonString, Class<T> clazz) {
        try {
            return (List) objMapperLocal.get().readValue(jsonString, TypeFactory.collectionType(List.class, clazz));
        } catch (Exception var3) {
            LOGGER.error("toList error: " + jsonString, var3);
            return null;
        }
    }

    public static Map<String, Object> toMap(String jsonString) {
        return (Map) toT(jsonString, Map.class);
    }

    public static String prettyPrint(Object value) {
        String result = null;

        try {
            result = objMapperLocal.get().writerWithDefaultPrettyPrinter().writeValueAsString(value);
        } catch (Exception var3) {
            LOGGER.error("prettyPrint error: " + value, var3);
        }

        if ("null".equals(result)) {
            result = null;
        }

        return result;
    }
}
