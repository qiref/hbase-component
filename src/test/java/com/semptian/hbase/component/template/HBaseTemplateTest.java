package com.semptian.hbase.component.template;

import com.semptian.hbase.component.HBaseApp;
import com.semptian.hbase.component.operations.HBaseOperations;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author YaoQi
 * @Date 2018/8/14 11:31
 * @Modified
 * @Description
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = HBaseApp.class)
public class HBaseTemplateTest {

    @Autowired
    private HBaseOperations hBaseDao;

    @Autowired
    private HBaseAdmin hBaseAdmin;

    /**
     * 查询测试
     */
    @Test
    public void testQueryTable() {
        Result result = hBaseDao.queryByTableNameAndRowKey("LBS", 9223372036854775803L);
        System.out.println(result.isEmpty());
        result.listCells().forEach(cell -> {
            System.out.println("row:" + Bytes.toLong(CellUtil.cloneRow(cell)) + "      family:"
                    + Bytes.toString(CellUtil.cloneFamily(cell)) + " qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + "    value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        });
    }

    @Test
    public void testQuery() {
        String tableName = "HBase-component";
        String rowKey = "1534154424340";
        Get get = new Get(rowKey.getBytes());
        List<Get> getList = new ArrayList<>();
        getList.add(get);
        System.out.println(hBaseDao.query(tableName, getList).length);
    }

    /**
     * 创建表测试
     */
    @Test
    public void testCreateTable() {
        hBaseDao.createTable("HBase-component", "CF1", "CF2");
    }

    /**
     * 删除表测试
     */
    @Test
    public void deleteTable() {
        hBaseDao.dropTable("HBase-component_1");
    }

    /**
     * 判断表是否存在
     */
    @Test
    public void testTableExist() {
        System.out.println(hBaseDao.tableExists("lbs"));
        System.out.println(hBaseDao.tableExists("LBS"));
        System.out.println(hBaseDao.tableExists("goods"));
        System.out.println(hBaseDao.tableExists("GOODS"));
    }

    /**
     * 插入一条记录
     */
    @Test
    public void putTest() {
        String rowKey = String.valueOf(System.currentTimeMillis());
        hBaseDao.put("HBase-component", "1534154424340", "CF1", "test_1", Bytes.toBytes("testData"));
    }

    /**
     * 批量插入数据
     */
    @Test
    public void testBatchPut() {
        String rowKey = String.valueOf(System.currentTimeMillis());
        Put put = new Put(rowKey.getBytes());
        String defaultColumn = "CF1";
        String column1 = "col1";
        String column2 = "col2";
        String column3 = "col3";

        String value = "test";
        put.addColumn(defaultColumn.getBytes(), column1.getBytes(), value.getBytes());
        put.addColumn(defaultColumn.getBytes(), column2.getBytes(), value.getBytes());
        put.addColumn(defaultColumn.getBytes(), column3.getBytes(), value.getBytes());

        List<Put> putList = new ArrayList<>();
        putList.add(put);
        putList.add(put);
        putList.add(put);
        putList.add(put);
        putList.add(put);

        hBaseDao.putBatch("HBase-component", putList);
    }

    @Test
    public void deleteTest() {
        hBaseDao.delete("HBase-component", "1534210201115", "CF1", "col2");
    }

    @Test
    public void deleteBatchTest() {
        String tableName = "HBase-component";
        String rowKey1 = "1534164113922";
        String rowKey2 = "1534168248328";

        List<Delete> deleteList = new ArrayList<>();
        Delete delete = new Delete(rowKey1.getBytes());
        Delete delete1 = new Delete(rowKey2.getBytes());
        deleteList.add(delete);
        deleteList.add(delete1);
        hBaseDao.deleteBatch(tableName, deleteList);
    }

    @Test
    public void testQueryScan() {

        String tableName = "RECOMMEND_ENGINE_DATA_MODEL";
        List<Long> timestampList = new ArrayList<>();
        Long timeDifference = 3L * 30L * 24L * 60L * 60L * 1000L;
        Long from = System.currentTimeMillis() - timeDifference;
        timestampList.add(from);
        TimestampsFilter timestampsFilter = new TimestampsFilter(timestampList);

        Scan scan = new Scan();
        try {
            scan.setTimeRange(from, System.currentTimeMillis());
        } catch (IOException e) {
            e.printStackTrace();
        }

        ResultScanner resultScanner = hBaseDao.queryByScan(tableName, scan);

        resultScanner.forEach(result -> {
            result.listCells().forEach(cell -> {
                System.out.println("row:" + Bytes.toLong(CellUtil.cloneRow(cell)) + "      family:"
                        + Bytes.toString(CellUtil.cloneFamily(cell)) + " qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "    value:" + Bytes.toString(CellUtil.cloneValue(cell)));

            });
        });
    }

    @Test
    public void testPageFilter() {
        String tableName = "HBase-component";
        Scan scan = new Scan();

        PageFilter pageFilter = new PageFilter(1);

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                "CF1".getBytes(),
                "col1".getBytes(),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("group"));

        singleColumnValueFilter.setFilterIfMissing(true);
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(pageFilter);

        scan.setFilter(filterList);

        ResultScanner resultScanner = hBaseDao.queryByScan(tableName, scan);

        try {
            resultScanner.forEach(result -> {
                result.listCells().forEach(cell -> {
                    System.out.println("row:" + Bytes.toString(CellUtil.cloneRow(cell)) + "      family:"
                            + Bytes.toString(CellUtil.cloneFamily(cell)) + " qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell))
                            + "    value:" + Bytes.toString(CellUtil.cloneValue(cell)));

                });
            });
        } finally {
            if (resultScanner != null) {
                resultScanner.close();
            }
        }
    }
}