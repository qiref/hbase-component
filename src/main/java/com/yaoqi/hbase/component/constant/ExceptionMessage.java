package com.yaoqi.hbase.component.constant;

/**
 * @Author YaoQi
 * @Date 2018/8/13 16:52
 * @Modified
 * @Description 异常信息常量类
 */
public class ExceptionMessage {

    /**
     * 参数不为空异常信息
     */
    public static final String NOT_NULL_MSG = "This argument is required. It must not be null!";

    /**
     * String 参数长度异常信息
     */
    public static final String HAS_LENGTH_MSG = "This String must have length. It must not be null or empty!";

    /**
     * 表名不存在异常信息
     */
    public static final String TABLE_NOT_EXISTS_MSG = "Table is not exists!";

    /**
     * 表名已存在异常信息
     */
    public static final String TABLE_ALREADY_EXISTS_MSG = "Table is already exists!";
}
