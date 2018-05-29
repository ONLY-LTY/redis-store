package com.redis.store.utils;

import java.util.regex.Matcher;

/**
 * Author LTY
 * Date 2018/05/23
 */
public class StringUtils extends org.apache.commons.lang3.StringUtils {

    public static String escape(String src) {
        StringBuilder tmp = new StringBuilder();
        tmp.ensureCapacity(src.length() * 6);
        for (int i = 0; i < src.length(); i++) {
            char c = src.charAt(i);
            if (Character.isDigit(c) || Character.isLowerCase(c) || Character.isUpperCase(c))
                tmp.append(c);
            else if (c < 256) {
                tmp.append("\\");
                if (c < 16)
                    tmp.append("0");
                tmp.append(Integer.toString(c, 16));
            } else {
                tmp.append("\\u");
                tmp.append(Integer.toString(c, 16));
            }
        }
        return tmp.toString();
    }

    public static String upperCaseFirstChar(String str) {
        return str.toUpperCase().substring(0, 1) + str.substring(1);
    }


    /**
     * replace '{i}' in the message with parameters, 'i' is the index of the param
     * eg. ("I am {0} years old", 21), then '21' will replace the '{i}' in the string
     * so the result is "I am 21 years old"
     * @param message
     * @param params
     */
    public static String format(String message, Object... params) {
        if (message == null) return null;
        if (params == null || params.length == 0) return message;

        for (int i = 0; i < params.length; i++) {
            // Matcher.quoteReplacement will fix the bug of replaceAll: No Group 5. quote "$" to "\$"
            message = message.replaceAll("\\{"+ i +"\\}", params[i] == null ? "null" : Matcher.quoteReplacement(String.valueOf(params[i].toString())));
        }

        return message;
    }

}
