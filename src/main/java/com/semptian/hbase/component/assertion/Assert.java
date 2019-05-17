package com.semptian.hbase.component.assertion;

import com.semptian.hbase.component.constant.ExceptionMessage;
import org.springframework.util.StringUtils;

/**
 * @Author YaoQi
 * @Date 2018/8/13 17:00
 * @Modified
 * @Description 参数断言
 */
public class Assert {


    /**
     * String 类型参数校验，参数不合法直接抛出异常
     *
     * @param text 参数
     */
    public static void hasLength(String text) {
        if (!StringUtils.hasLength(text)) {
            throw new IllegalArgumentException(ExceptionMessage.HAS_LENGTH_MSG);
        }
    }

    /**
     * 参数批量判断是否为空
     *
     * @param text 可传多个参数
     */
    public static void hasLengthBatch(String... text) {
        for (int i = 0; i < text.length; i++) {
            hasLength(text[i]);
        }
    }

    /**
     * Object类型参数校验，参数不合法直接抛出异常
     *
     * @param object 对象类型参数
     */
    public static void notNull(Object object) {
        if (object == null) {
            throw new IllegalArgumentException(ExceptionMessage.NOT_NULL_MSG);
        }
    }

    /**
     * 参数批量判断
     *
     * @param objects 多个参数集合
     */
    public static void notNullBatch(Object... objects) {
        for (int i = 0; i < objects.length; i++) {
            notNull(objects[i]);
        }
    }
}
