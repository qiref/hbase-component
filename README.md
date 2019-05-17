# HBase 组件接口文档

----

#### 使用说明

基本概念

table: 表

columnFamily:列族，一个表下可以有多个列族，但是不建议设置多个列族，HBase建议设计长窄型的表而不是短宽型。

qualifier:列，一个列族下可以有多列，一个表中的列可以是不对齐的，但是这样效率不高，同一张表中的列最好是相同的。 

cell:一列数据下的一个单元格，一个列下可以有多个单元格，根据版本号区分，默认每次读取最新版本的数据，cell下的存储是数据本身。

row: 行，多列数据组成一行，一行中有多个qualifier。

rowKey: 行健，用于唯一标识一行数据，一行下有多列，行健的设计直接关系到查询的效率。

### HBase配置

以下配置为最基础配置，缺一不可。

``` yml
HBase:
  conf:
    quorum: 192.168.80.234:2181,192.168.80.235:2181,192.168.80.241:2181
    znodeParent: /hbase-unsecure
    #如果有更多配置，写在config下，例如：
    #config:
    #  key: value
    #  key: value
```

如果需要更多配置，需要在config中配置，以key-value的形式书写。


### 参数说明

quorum是HBase中zookeeper的配置，znodeParent是HBase配置在zookeeper中的路径。

## 简单示例

引入组件jar包：

``` xml
        <dependency>
            <groupId>com.semptian.hbase.component</groupId>
            <artifactId>hbase-component</artifactId>
            <version>1.0.1-SNAPSHOT</version>
        </dependency>
```

在需要的地方注入HBaseOperations接口，该接口的实现类是HBaseTemplate，通过这个类来操作HBase。

``` java
@Autowired
    private HBaseOperations hBaseDao;
```

查询一条数据，通过rowKey查询：

``` java
public void testQueryTable() {
        Result result = hBaseDao.queryByTableNameAndRowKey(
            "LBS", 9223372036854775803L);
        System.out.println(result.isEmpty());
        result.listCells().forEach(cell -> {
            System.out.println(
            "row:" + Bytes.toLong(CellUtil.cloneRow(cell)) + 
            ",family:"+ Bytes.toString(CellUtil.cloneFamily(cell)) +
            ", qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
            ", value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        });
    }

```

## 表的基本操作

### 新建表

创建表通过HBaseTemplate就可以实现，HBaseTemplate类中带有这个方法。

操作示例：
``` java
hBaseDao.createTable("HBASE-COMPONENT_1", "CF1", "CF2");
```
上述代码创建了一张表，HBASE-COMPONENT_1 是表名，CF1,CF2代表这个表有两个列族。

如果有多个列族可以往后面加，列族不建议设置很多个。

### 删除表

``` java
hBaseDao.dropTable("HBASE-COMPONENT_1");
```
参数是表名，通过表名删除表。

### 判断表是否存在

``` java
hBaseDao.tableExists("lbs");
```
这里的表名是区分大小写的。返回值：boolean。

### 新增数据

#### 新增一条数据

需要注意的是在HBase中的存储的数据是不分格式的，都是以字节数组的形式存储，因此在存储一条数据时需要将数据都转化成字节数组。

String格式的数据能直接转换为字节数组getBytes()，但是其他格式的数据需要借助工具作转换。

这里需要格外注意rowKey的格式，用什么格式存就决定了用什么格式取。
``` java
hBaseDao.put("HBase-component", "1534154424340", "CF1", "test_1", Bytes.toBytes("testData"));
```

参数说明：
``` java
(1) tableName  目标数据表
(2) rowName    rowKey
(3) familyName 列族名
(4) qualifier  列名
(5) data       字节数组类型的数据

```
这里新增一条数据是填充数据到一个cell中去。

#### 批量新增数据

``` java
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
```
批量插入数据就是使用多个Put对象，putBatch(...)方法的参数：表名，putList(多个put的集合)。
注意批量插入数据也都是插入字节数组格式的数据。

### 删除数据

#### 删除一条数据

``` java
hBaseDao.delete("HBase-component", "1534210201115", "CF1", "col2");
```
参数说明：

(1) 表名

(2) rowKey

(3) 列族名

(4) 列名

这里删除是删除一个cell下的数据

#### 批量删除数据

``` java
String tableName = "HBase-component";
String rowKey1 = "1534164113922";
String rowKey2 = "1534168248328";

List<Delete> deleteList = new ArrayList<>();
Delete delete = new Delete(rowKey1.getBytes());
Delete delete1 = new Delete(rowKey2.getBytes());
deleteList.add(delete);
deleteList.add(delete1);
hBaseDao.deleteBatch(tableName, deleteList);
```
批量删除需要借助Delete对象。

### 查询

### 单条结果查询
``` java
Result result = hBaseDao.queryByTableNameAndRowKey("LBS", 9223372036854775803L);
        System.out.println(result.isEmpty());
        result.listCells().forEach(cell -> {
            System.out.println(
                " row:" + Bytes.toLong(CellUtil.cloneRow(cell)) + 
                " family:"+ Bytes.toString(CellUtil.cloneFamily(cell)) + 
                " qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)) + 
                " value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        });
```
queryByTableNameAndRowKey()该方法是通过表名和rowKey查询数据，这里的rowKey支持多种类型，Long，double，Integer几种类型。
至于这里传什么类型的参数，取决于插入数据时rowKey的类型，虽然HBase里存储的都是字节数组，但是对类型是敏感的，如果类型对不上可能会出错。

### 批量扫描

``` java
// 构建scan
Scan scan = new Scan();

// 设置时间戳,计算时间差
Long timeDifference = 2L * 30L * 24L * 60L * 60L * 1000L;
Long endTime = System.currentTimeMillis();
Long fromTime = endTime - timeDifference;

// 设置时间过滤器
FilterList filterList = new FilterList();
Filter startTimeFilter = new SingleColumnValueFilter(
    DEFAULT_COLUMN_FAMILY.getBytes(),
    DATA_CREATE_TIME.getBytes(),
    CompareFilter.CompareOp.GREATER,
    Bytes.toBytes(fromTime)
);

Filter endTimeFilter = new SingleColumnValueFilter(
    DEFAULT_COLUMN_FAMILY.getBytes(),
    DATA_CREATE_TIME.getBytes(),
    CompareFilter.CompareOp.LESS,
    Bytes.toBytes(endTime)
);


filterList.addFilter(startTimeFilter);
filterList.addFilter(endTimeFilter);

scan.setFilter(filterList);

// 获取结果集
ResultScanner resultScanner = hBaseTemplate.queryByScan(TABLE_NAME, scan);

// 遍历结果集
try{
    if (resultScanner != null) {
        resultScanner.forEach(result -> {
            List<Cell> cellList = result.listCells();
                ...
            }
    }
}finally{
    if (resultScanner != null) {
        resultScanner.close();
    }
}

```

批量查询可以通过queryByScan()方法实现，第一个参数是表名，第二个参数是scan，通过构建不同的scan来查询，过滤器也是在构建scan对象是添加的，可以添加多个过滤器。

*需要注意的是这里的ResultScanner类，在遍历结果集时需要使用try-finally结构，在使用完resultScanner对象之后关闭该对象。HBase官方文档上强调了这一点。因此在使用ResultScanner对象时需要格外注意。*

常见过滤器：

行健过滤器：RowFilter

列族过滤器：FamilyFilter

值过滤器：ValueFilter

列过滤器：QualifierFilter

单列值过滤器：SingleColumnValueFilter(会返回满足条件的行)

单列值排除过滤器：SingleColumnExcludeFilter(返回排除了该列的结果，与单列值过滤器相反)

前缀过滤器：PrefixFilter(这个过滤器是针对行健的，在构造方法中传入字节数组形式的内容，过滤器会去匹配行健)

页数过滤器：PageFilter(使用pageFilter过滤器的时候需要注意，并不是设置了页数大小就能返回相应数目的结果)

``` java
String tableName = "RECOMMEND_ENGINE_DATA_MODEL";
Scan scan = new Scan();

PageFilter pageFilter = new PageFilter(1);
scan.setFilter(pageFilter);

ResultScanner resultScanner = hBaseDao.queryByScan(tableName, scan);

try{
    resultScanner.forEach(result -> {
          result.listCells().forEach(cell -> {
               // process    
          });
}finally{
    if (resultScanner != null) {
          resultScanner.close();
    }
}
```

上面这段代码中设置了页面大小为1，预期是返回一条数据，但是结果会返回两条数据，这时返回的结果数会取决于regionServer的数量。

如果是FilterList，FilterList的顺序会影响PageFilter的效果。

一般比较型过滤器，需要用CompareFilter.CompareOp中的比较运算符。所有的过滤器都是用Scan对象去设置。

#### 多过滤器查询
``` java
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
              System.out.println(
                  " row:" + Bytes.toString(CellUtil.cloneRow(cell)) + 
                  " family:"+ Bytes.toString(CellUtil.cloneFamily(cell)) + 
                  " qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell))+ 
                  " value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                });
            });
    } finally {
         if (resultScanner != null) {
             resultScanner.close();
         }
    }
```

多过滤器需要用到FilterList，也是直接设置到Scan对象中。多过滤器的时候需要注意过滤器的顺序问题，例如上面代码中如果将两个过滤器调换顺序，查询的结果也是不一样的。

### 结果集的映射

在HBase中，默认所有的顺序都是按照字母序排列，例如CF1列族下有多个列：col1、col2、col3，那么在遍历结果集时，listCells()中的cell的顺序总是按照列名的字母序来排列的。

所以cellList.get(0)就是对应col1中的数据，cellList.get(1)就是对应col2中的数据，cellList.get(2)就是对应col3中的数据。

如果列名为a、b、c那分别对应的下标为cellList.get(0)、cellList.get(1)、cellList.get(2)