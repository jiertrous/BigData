package com.bigtable.consumer;

import com.bigtable.Constant.Constant;
import com.bigtable.utils.HBaseUtil;
import com.bigtable.utils.PropertityUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 1.初始化命名空间
 * 2.创建表
 * 3.批量存储数据
 */
public class HBaseDAO {
    // 配置信息
    private Properties properties;
    // 命名空间
    private String nameSpace;
    // 表名
    private String tableName;
    // 分区数
    private int regions;
    // 列族
    private String cf;
    // 初始化
    private SimpleDateFormat sdf;
    // HBase连接配置信息
    private Connection connection;
    // HBase表对象
    private Table table;
    //
    private List<Put> puts;
    // 主被叫标志服
    private String flag;


    public HBaseDAO() throws IOException {
        // 初始化相应的参数
        properties = PropertityUtil.getPropertity();
        nameSpace = properties.getProperty("hbase.namespace");
        tableName = properties.getProperty("hbase.table.name");
        regions = Integer.valueOf(properties.getProperty("hbase.regions"));
        cf = properties.getProperty("hbase.cf");
        sdf = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
        connection = ConnectionFactory.createConnection(Constant.CONF);
        table = connection.getTable(TableName.valueOf(tableName));
        puts = new ArrayList<Put>();
        flag = "1";

        // 初始化命名空间,测试的时候注释掉，测试命名空间过于麻烦
       // HBaseUtil.createNamespace(nameSpace);
        // 创建表
        HBaseUtil.createTable(tableName, regions, cf,"f2");
    }

    // 批量提交数据，达到一定的条数发送
    public void put(String value) throws ParseException, IOException {
        if (value == null) {
            return;
        }
//        if (puts.size() == 0){
//
//        }

        //18302820904,15064972307,2019-04-01 20:43:18,0992
        String[] split = value.split(",");

        //第一个号码
        String call1 = split[0];
        //第二个号码
        String call2 = split[1];
        //通话建立时间
        String buildTime = split[2];
        //通话时长
        String duration = split[3];

        //通话建立时间时间戳形式
        long buildTS = sdf.parse(buildTime).getTime();

        // 生成分区号
        String rowHash = HBaseUtil.getRowHash(regions, call1, buildTime);

        // 生成rowkey
        String rowkey = HBaseUtil.getRowKey(rowHash, call1, buildTime,
                buildTS + "", call2, flag,duration);

        // 生成put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildTS"), Bytes.toBytes(buildTS + ""));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        // 用协处理器添加f2分区，防止在一个线程内多次插入，
        puts.add(put);
        // 20提交一次
        if (puts.size() > 20) {
            table.put(puts);
            puts.clear();
        }
        // 只有一张表，关掉资源毫无意义
        // table.close();
        // 资源浪费
//        Connection connection = ConnectionFactory.createConnection();
//        Table table = connection.getTable(TableName.valueOf(tableName));
//        table.put(put);
    }

    public void close() throws IOException {
        table.put(puts);
        table.close();
        connection.close();
    }
}
