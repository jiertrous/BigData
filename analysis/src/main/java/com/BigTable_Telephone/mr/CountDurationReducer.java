package com.BigTable_Telephone.mr;

import com.BigTable_Telephone.kv.CommDimension;
import com.BigTable_Telephone.kv.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountDurationReducer extends Reducer<CommDimension, Text, CommDimension, CountDurationValue> {

    private CountDurationValue v = new CountDurationValue();

    @Override
    protected void reduce(CommDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //初始化通话次数&通话时长
        int counter = 0;
        int durations = 0;

        //循环累加
        for (Text value : values) {
            counter++;
            durations += Integer.valueOf(value.toString());
        }
        //设置通话次数&通话时长
        v.setCount(counter);
        v.setDuration(durations);

        //将数据写出
        context.write(key, v);
    }
}