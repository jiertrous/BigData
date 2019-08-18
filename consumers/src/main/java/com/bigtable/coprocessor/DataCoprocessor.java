package com.bigtable.coprocessor;

import com.bigtable.Constant.Constant;
import com.bigtable.utils.HBaseUtil;
import com.bigtable.utils.PropertityUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DataCoprocessor extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {
        super.postPut(e, put, edit, durability);
        // 获取协处理器
        String newTable = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        // 当前操作的表
        String oldTable = PropertityUtil.getPropertity().getProperty("hbase.table.name");
        // old和new如果相等，则hbase挂掉，old与new表不同
        if (!newTable.equals(oldTable)) {
            return;
        }
        String oldrowKey = Bytes.toString(put.getRow());
        // 切分
        String[] split = oldrowKey.split("_");

        // flag过滤被叫数据，1主叫
        if ("0".equals(split[5])) {
            return;
        }
        // 0为hash，获取所有字段，不需要获取值，值在rowkey
        String caller = split[1];
        String buildTime = split[2];
        String buildTS = split[3];
        String callee = split[4];
        String duration = split[6];
        int regions = Integer.valueOf(PropertityUtil.getPropertity().getProperty("hbase.regions"));
        // 生成新的分区号
        String rowHash = HBaseUtil.getRowHash(regions, callee, buildTime);
        // 生成被叫rowkey
        String newrowKey = HBaseUtil.getRowKey(rowHash, callee, buildTime, buildTS, caller, "0", duration);

        //添加数据
        // 如果只接电话，没有主动打电话，也需要提供查询功能，
        Put newPut = new Put(Bytes.toBytes(newrowKey));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTS"), Bytes.toBytes(buildTS + ""));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //获取HBase连接以及表对象
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table table = connection.getTable(TableName.valueOf(oldTable));

        //插入被叫数据
        table.put(newPut);
        //关闭资源
        table.close();
        connection.close();

    }
}
