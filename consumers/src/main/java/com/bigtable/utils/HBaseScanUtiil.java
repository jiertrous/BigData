package com.bigtable.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

// 生成多组startrow和stoprow
public class HBaseScanUtiil {
    // 队列算法

    static List<String[]> list;
    static SimpleDateFormat sdf;

    // 18706287692,15133295266,2019-05-25 15:00:31,0683
//    0x_18706287692_2019-05
//    0x_18706287692_2019-06
//    0x_18706287692_2019-06
//    0x_18706287692_2019-07
    public static List<String[]> getStartStop(String phoneNum, String start, String stop) throws ParseException {
        // 用户传进来start和stop的值，开始结束时间4，6
        list = new ArrayList<>();

        // 变为日期类
        sdf = new SimpleDateFormat("yyyy-MM");
        Date startDate = sdf.parse(start);  // 4
        Date stopDate = sdf.parse(stop);    // 6

        Calendar startPoint = Calendar.getInstance();
        startPoint.setTime(startDate);  // 4
        // 一组一组的循环，一组为一个月
        Calendar stopPoint = Calendar.getInstance();
        stopPoint.setTime(startDate);   // 5
        stopPoint.add(Calendar.MONTH, 1);

        // <= 4-7 4 5 6
        while (startPoint.getTimeInMillis() <= stopDate.getTime()) {
            //2019-04   2019-05     2019-06
            String buildTime = sdf.format(startPoint.getTime());
            String stopTime = sdf.format(stopPoint.getTime());

            String rowHash = HBaseUtil.getRowHash(6, phoneNum, buildTime);
            String startRow = rowHash + "_" + phoneNum + "_" + buildTime; // 开始时间
            String stopRow = rowHash + "_" + phoneNum + "_" + stopTime;  // 结束时间

            list.add(new String[]{startRow, stopRow});

            startPoint.add(Calendar.MONTH, 1);
            stopPoint.add(Calendar.MONTH, 1);
        }
        return list;
    }

    private static int i = 0;

    public static boolean hasNext() {
        return i < list.size();
    }

    public static String[] next() {
        return list.get(i++);// i从零开始
    }

}
