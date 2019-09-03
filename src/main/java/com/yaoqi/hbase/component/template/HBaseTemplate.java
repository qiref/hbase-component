package com.yaoqi.hbase.component.template;

import com.yaoqi.hbase.component.assertion.Assert;
import com.yaoqi.hbase.component.constant.CommonConstant;
import com.yaoqi.hbase.component.constant.ExceptionMessage;
import com.yaoqi.hbase.component.operations.HBaseOperations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @Author YaoQi
 * @Date 2018/8/13 14:05
 * @Modified
 * @Description
 */
@Component
public class HBaseTemplate implements HBaseOperations {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTemplate.class);

    @Autowired
    private Connection connection;

    /**
     * 判断表名是否存在
     *
     * @param tableName 表名 String ,注意这里区分大小写
     * @return
     */
    @Override
    public boolean tableExists(String tableName) {
        Table table = null;
        boolean tableExistsFlag = false;
        try (Admin admin = connection.getAdmin()) {
            table = connection.getTable(TableName.valueOf(tableName));
            tableExistsFlag = admin.tableExists(table.getName());
        } catch (IOException e) {
            logger.error("IOException : {}", e.getMessage());
        } finally {
            closeTable(table);
        }
        return tableExistsFlag;
    }

    /**
     * 通过表名和rowKey获取数据,获取一条数据
     *
     * @param tableName 表名
     * @param rowKeyVar rowKey 泛型 可支持多种类型{String,Long,Double}
     * @return Result 类型
     */
    @Override
    public <T> Result queryByTableNameAndRowKey(String tableName, T rowKeyVar) {
        Assert.notNullBatch(tableName, rowKeyVar);
        Assert.hasLength(tableName);
        boolean tableExists = tableExists(tableName);
        if (!tableExists) {
            logger.info("{}" + ExceptionMessage.TABLE_NOT_EXISTS_MSG, tableName);
            return null;
        }
        byte[] rowKey = checkType(rowKeyVar);
        Result result = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey);
            result = table.get(get);
        } catch (IOException e) {
            logger.error("IOException : {}", e.getMessage());
        } finally {
            closeTable(table);
        }
        return result;
    }

    /**
     * 自定义查询
     *
     * @param tableName 表名
     * @param getList   请求体
     * @return Result类型
     */
    @Override
    public Result[] query(String tableName, List<Get> getList) {
        Assert.notNullBatch(tableName, getList);
        Assert.hasLength(tableName);
        boolean tableExists = tableExists(tableName);
        if (!tableExists) {
            logger.info("{}" + ExceptionMessage.TABLE_NOT_EXISTS_MSG, tableName);
            return null;
        }
        Table table = null;
        Result[] result = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            result = table.get(getList);
        } catch (IOException e) {
            logger.error("query error , message:{}", e.getMessage());
        } finally {
            closeTable(table);
        }
        return result;
    }


    /**
     * 传入一个泛型V 判断基本类型，返回对应的byte数组
     *
     * @param rowKeyVar 泛型rowKey
     * @param <V>       泛型
     * @return 返回格式化后的字节数组
     */
    private <V> byte[] checkType(V rowKeyVar) {
        byte[] rowKey = new byte[0];
        //判断T的类型
        String rowKeyType = rowKeyVar.getClass().getTypeName();
        logger.info("rowKey Type is : {}", rowKeyType);
        if (String.class.getName().equals(rowKeyType)) {
            rowKey = Bytes.toBytes((String) rowKeyVar);
        }
        if (Long.class.getName().equals(rowKeyType)) {
            rowKey = Bytes.toBytes((Long) rowKeyVar);
        }
        if (Double.class.getName().equals(rowKeyType)) {
            rowKey = Bytes.toBytes((Double) rowKeyVar);
        }
        if (Integer.class.getName().equals(rowKeyType)) {
            rowKey = Bytes.toBytes((Integer) rowKeyVar);
        }

        return rowKey;
    }

    /**
     * 关闭连接
     *
     * @param table 表名
     */
    private void closeTable(Table table) {
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("close table {} error {}", table.getName(), e.getMessage());
            }
        } else {
            logger.info("table is null");
        }
    }

    /**
     * 创建一张表
     *
     * @param tableName  表名
     * @param familyName 列族名
     */
    @Override
    public void createTable(String tableName, String... familyName) {
        createTable(tableName, Arrays.asList(familyName), null);
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param familyName
     * @param splitkeys
     */
    @Override
    public void createTable(String tableName, List<String> familyName, byte[][] splitkeys) {
        Assert.notNullBatch(tableName, familyName);
        Assert.hasLength(tableName);
        TableName tableNameVar = TableName.valueOf(tableName);
        if (!tableExists(tableName)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableNameVar);
            for (String aFamilyName : familyName) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(aFamilyName);
                hColumnDescriptor.setBlockCacheEnabled(true);
                hColumnDescriptor.setPrefetchBlocksOnOpen(true);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            try (Admin admin = connection.getAdmin()) {
                if (splitkeys != null) {
                    admin.createTable(hTableDescriptor, splitkeys);
                } else {
                    admin.createTable(hTableDescriptor);
                }

            } catch (IOException e) {
                logger.error("create failed , Exception: {}", e.getMessage());
            }
        } else {
            throw new IllegalArgumentException(ExceptionMessage.TABLE_ALREADY_EXISTS_MSG);
        }
    }

    /**
     * 新增一条数据
     *
     * @param tableName  目标数据表
     * @param rowName    rowKey
     * @param familyName 列族名
     * @param qualifier  列名
     * @param data       字节数组类型的数据
     */
    @Override
    public void put(String tableName, String rowName, String familyName, String qualifier, byte[] data) {
        Assert.notNullBatch(tableName, rowName, familyName, qualifier);
        Assert.hasLengthBatch(tableName, rowName, familyName, qualifier);
        Table table = null;
        if (tableExists(tableName)) {
            try {
                table = connection.getTable(TableName.valueOf(tableName));
                Put put = new Put(Bytes.toBytes(rowName));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), data);
                table.put(put);
            } catch (IOException e) {
                logger.error("data put error,message: {}", e.getMessage());
            } finally {
                closeTable(table);
            }
        } else {
            throw new IllegalArgumentException(ExceptionMessage.TABLE_NOT_EXISTS_MSG);
        }
    }

    /**
     * 批量插入数据
     *
     * @param tableName 表名
     * @param putList   put集合
     */
    @Override
    public void putBatch(String tableName, List<Put> putList) {
        Assert.notNull(putList);
        Assert.hasLength(tableName);
        // 限制最大插入数量
        if (putList.size() > CommonConstant.BATCH_PUT_LIMIT) {
            logger.warn("putList is too large to insert, {} is limit，The real size is {}",
                    CommonConstant.BATCH_PUT_LIMIT, putList.size());
        } else {
            Table table = null;
            if (tableExists(tableName)) {
                try {
                    table = connection.getTable(TableName.valueOf(tableName));
                    table.put(putList);
                } catch (IOException e) {
                    logger.error("data put error, message:{}", e.getMessage());
                } finally {
                    closeTable(table);
                }
            } else {
                throw new IllegalArgumentException(ExceptionMessage.TABLE_NOT_EXISTS_MSG);
            }
        }
    }

    /**
     * 删除一个列族下的数据
     *
     * @param tableName  目标数据表
     * @param rowName    rowKey
     * @param familyName 列族名
     */
    @Override
    public void delete(String tableName, String rowName, String familyName) {
        delete(tableName, rowName, familyName, null);
    }

    /**
     * 删除某个列下的数据
     *
     * @param tableName  目标数据表
     * @param rowName    rowKey
     * @param familyName 列族名
     * @param qualifier  列名
     */
    @Override
    public void delete(String tableName, String rowName, String familyName, String qualifier) {
        Assert.notNullBatch(tableName, rowName, familyName);
        Assert.hasLengthBatch(tableName, rowName, familyName);
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(rowName.getBytes());
            if (qualifier != null) {
                delete.addColumn(familyName.getBytes(), qualifier.getBytes());
            }
            table.delete(delete);
        } catch (IOException e) {
            logger.error("data delete error, message:{}", e.getMessage());
        } finally {
            closeTable(table);
        }
    }

    /**
     * 批量删除数据
     *
     * @param tableName  表名
     * @param deleteList 需要删除的数据
     */
    @Override
    public void deleteBatch(String tableName, List<Delete> deleteList) {
        Assert.notNull(tableName);
        Assert.hasLength(tableName);
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            table.delete(deleteList);
        } catch (IOException e) {
            logger.error("data delete error, message:{}", e.getMessage());
        } finally {
            closeTable(table);
        }
    }

    /**
     * 通过scan查询数据
     *
     * @param tableName 表名
     * @param scan      scan
     * @return 返回 ResultScanner
     */
    @Override
    public ResultScanner queryByScan(String tableName, Scan scan) {
        Assert.notNullBatch(tableName, scan);
        Assert.hasLength(tableName);
        ResultScanner resultScanner = null;
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            logger.error("query error, message:", e.getMessage());
        } finally {
            closeTable(table);
        }
        return resultScanner;
    }

    /**
     * 删除表
     *
     * @param tableName 表名称
     */
    @Override
    public void dropTable(String tableName) {
        boolean tableExists = tableExists(tableName);
        if (tableExists) {
            try (Admin admin = connection.getAdmin()) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                logger.info("table {} delete successfully", tableName);
            } catch (IOException e) {
                logger.error("class:[{}] method:[{}] excuter throw exception:[{}]", this.getClass().getName(), "dropTable", e.getMessage());
            }
        } else {
            logger.info("table {} is not exists", tableName);
        }
    }

    @Override
    public void dropTable(List<String> tableNames) {
        try (Admin admin = connection.getAdmin()) {
            tableNames.stream().distinct().forEach(tableName -> {
                boolean tableExists = tableExists(tableName);
                if (tableExists) {
                    try {
                        admin.disableTable(TableName.valueOf(tableName));
                        admin.deleteTable(TableName.valueOf(tableName));
                        logger.info("table {} delete successfully", tableName);
                    } catch (IOException e) {
                    }
                } else {
                    logger.info("table {} is not exists", tableName);
                }
            });
        } catch (IOException e) {
            logger.error("class:[{}] method:[{}] excuter throw exception:[{}]", this.getClass().getName(), "dropTable", e.getMessage());
        }
    }

    @Override
    public void truncateTable(String tableName) throws IOException {
        boolean tableExists = tableExists(tableName);
        if (tableExists) {
            Admin admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.truncateTable(TableName.valueOf(tableName), false);
            logger.info("table {} truncate successfully", tableName);
        } else {
            logger.info("table {} is not exists", tableName);
        }
    }

    @Override
    public boolean deleteColumn(String tableName, String family, String rowKey, String column) throws IOException {
        return this.deleteColumn(tableName, family, rowKey, Arrays.asList(column));
    }

    @Override
    public boolean deleteColumn(String tableName, String family, String rowKey, List<String> columns) throws IOException {
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family) || StringUtils.isBlank(rowKey)) {
            logger.info("parameter is error");
            return false;
        }
        if (columns.isEmpty()) {
            logger.info("colums is empty");
            return false;
        }
        boolean tableExists = tableExists(tableName);
        if (tableExists) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(rowKey.getBytes());
            for (String column : columns) {
                delete.addColumn(family.getBytes(), column.getBytes());
            }
            table.delete(delete);
            logger.info("table {} truncate successfully", tableName);
            return true;
        } else {
            logger.info("table {} is not exists", tableName);
            return false;
        }
    }
}
