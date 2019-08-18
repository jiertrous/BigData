package com.bigtable.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表
 * 4.生成rowkey
 * 5.预分区键的生成
 */
public class HBaseUtil {
    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
    }

    //1.创建命名空间
    public static void createNamespace(String namespace) throws IOException {
        // 获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        // 获取命名空间描述其
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.
                create(namespace).build();
        // 创建命名空间
        admin.createNamespace(namespaceDescriptor);

        // 关闭资源
        admin.close();
        connection.close();
    }

    // 2.判断表是否存在
    public static boolean exisTable(String tableName) throws IOException {
        // 获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        // 判断
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        // 关闭资源
        admin.close();
        connection.close();

        return tableExists;
    }

    // 3.创建表
    public static void createTable(String tableName, int regions, String... cfs)
            throws IOException {
        // 获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        // 判断表是否存在
        if (exisTable(tableName)) {
            System.out.println("表：" + tableName + "已存在！");
            // 关闭资源
            admin.close();
            connection.close();
            return;
        }
        // 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 循环添加列族
        for (String cf : cfs) {
            // 创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        // 协处理器
        hTableDescriptor.addCoprocessor("com.bigtable.coprocessor.DataCoprocessor");

        // 创建表
        admin.createTable(hTableDescriptor, getSplitKeys(regions));
        // 关闭资源
        admin.close();
        connection.close();
    }

    // 预分区键生成,自己切分
    // 00|,01|,02|,03|,04|,05|，3个机器，根绝regions的个数，模运算
    private static byte[][] getSplitKeys(int regions) {
        // 创建分区键二位数组
        byte[][] splitKeys = new byte[regions][];
        DecimalFormat df = new DecimalFormat("00");
        // 循环添加分区键
        for (int i = 0; i < regions; i++) {
            splitKeys[i] = Bytes.toBytes(df.format(i) + "|");
        }
        return splitKeys;
    }

    // 生成rowkey,生成分区号，为了让不同的rowkey分散到region中
    // 0x_13712341234_2019-02-04 12:33:51_时间戳_1389879878_duration
    public static String getRowKey(String rowHash, String caller, String buildTime,
                                   String buildTS, String callee, String flag, String duration) {
        return rowHash + "_"
                + caller + "_"
                + buildTime + "_"
                + buildTS + "_"
                + callee + "_"
                + flag + "_"
                + duration;
    }

    // 生成分区好  取出年月，保证同一个人同一个年月进入一个region
    // 同一个人同一个月的放进一个regions里面，为了后面的需求
    public static String getRowHash(int regions, String caller, String buildTime) {
        DecimalFormat df = new DecimalFormat("00");
        // 取中间四位手机号
        String phoneMid = caller.substring(3, 7);
        String yearMonth = buildTime.replace("-", "").substring(0, 6);
        // 防止数据倾斜，异或，哈希算法，0x，x导致，调整x
        int i = (Integer.valueOf(phoneMid) ^ Integer.valueOf(yearMonth)) % regions;
        return df.format(i);
    }
        //  测试
//    public static void main(String[] args) {
//        byte[][] splitKeys = getSplitKeys(6);
//        for (byte[] splitKey : splitKeys) {
//            System.out.println(Bytes.toString(splitKey) + "---");
//        }
//        String rowHash = getRowHash(6,
//                "15595505996",
//                "2019-03-18 05:29:11");
//        System.out.println(getRowKey(rowHash, "15595505995",
//                "2019-03-18 05:29:11",
//                "4545",
//                "15178485516",
//                "0235"));
//    }

}
