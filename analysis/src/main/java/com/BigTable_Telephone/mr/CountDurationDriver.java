package com.BigTable_Telephone.mr;

import com.BigTable_Telephone.OutputFormat.SqlOutPutFormat;
import com.BigTable_Telephone.kv.CommDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class CountDurationDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置信息&job对象
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration);

        //2.设置jar所在路径
        job.setJarByClass(CountDurationDriver.class);

        //3.设置Mapper相应属性
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("f1"));
        TableMapReduceUtil.initTableMapperJob("ns_telecom:calllog",
                scan,
                CountDurationMapper.class,
                CommDimension.class,
                Text.class,
                job);

        //4.设置Reducer
        job.setReducerClass(CountDurationReducer.class);

        //5.设置OutPutFormat
        job.setOutputFormatClass(SqlOutPutFormat.class);

        //6.提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
