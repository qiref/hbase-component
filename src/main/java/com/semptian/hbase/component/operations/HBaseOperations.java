package com.semptian.hbase.component.operations;

import org.apache.hadoop.hbase.client.*;

import java.util.List;

/**
 * @Author YaoQi
 * @Date 2018/8/13 14:46
 * @Modified
 * @Description 组件接口
 */
public interface HBaseOperations {

    /**
     * 创建一张表
     *
     * @param tableName  表名
     * @param familyName 列族名
     */
    void createTable(final String tableName, final String... familyName);

    /**
     * 通过表名和rowKey获取数据
     *
     * @param tableName 表名
     * @param rowKeyVar rowKey 泛型 可支持多种类型{String,Long,Double}
     * @return Result 类型
     */
    <T> Result queryByTableNameAndRowKey(String tableName, T rowKeyVar);

    /**
     * 自定义查询
     *
     * @param tableName 表名
     * @param getList   请求体
     * @return Result类型
     */
    Result[] query(String tableName, List<Get> getList);

    /**
     * 判断表名是否存在
     *
     * @param tableName 表名 String ,注意这里区分大小写
     * @return
     */
    boolean tableExists(String tableName);

    /**
     * 新增一条数据
     *
     * @param tableName  目标数据表
     * @param rowName    rowKey
     * @param familyName 列族名
     * @param qualifier  列名
     * @param data       字节数组类型的数据
     */
    void put(final String tableName, final String rowName, final String familyName, final String qualifier, final byte[] data);

    /**
     * 批量插入数据
     *
     * @param tableName 表名
     * @param putList   put集合
     */
    void putBatch(final String tableName, List<Put> putList);

    /**
     * 删除一个列族下的数据
     *
     * @param tableName  target table
     * @param rowName    row name
     * @param familyName family
     */
    void delete(final String tableName, final String rowName, final String familyName);

    /**
     * 删除某个列下的数据
     *
     * @param tableName  目标数据表
     * @param rowName    rowKey
     * @param familyName 列族名
     * @param qualifier  列名
     */
    void delete(final String tableName, final String rowName, final String familyName, final String qualifier);

    /**
     * 批量删除数据
     *
     * @param tableName  表名
     * @param deleteList 需要删除的数据
     */
    void deleteBatch(final String tableName, List<Delete> deleteList);

    /**
     * 通过scan查询数据
     *
     * @param tableName 表名
     * @param scan      scan
     * @return 返回 ResultScanner
     */
    ResultScanner queryByScan(final String tableName, Scan scan);

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    void dropTable(String tableName);
}
