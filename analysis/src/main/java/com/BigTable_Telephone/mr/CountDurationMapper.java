package com.BigTable_Telephone.mr;

import com.BigTable_Telephone.kv.CommDimension;
import com.BigTable_Telephone.kv.ContactDimension;
import com.BigTable_Telephone.kv.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;

public class CountDurationMapper extends TableMapper<CommDimension, Text> {

    //公共维度
    private CommDimension commDimension = new CommDimension();
    //待写出的值
    private Text v = new Text();
    //联系人维度
    private ContactDimension contactDimension = new ContactDimension();
    //联系人信息
    private HashMap<String, String> contacts = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        contacts = new HashMap<>();
        contacts.put("15369468720", "李雁");
        contacts.put("19920860202", "卫艺");
        contacts.put("18411925860", "仰莉");
        contacts.put("14473548449", "陶欣悦");
        contacts.put("18749966182", "施梅梅");
        contacts.put("19379884788", "金虹霖");
        contacts.put("15195659735", "魏明艳");
        contacts.put("18503558939", "华贞");
        contacts.put("13407209608", "华啟倩");
        contacts.put("15596505995", "仲采绿");
        contacts.put("17519874292", "卫丹");
        contacts.put("15178485516", "戚丽红");
        contacts.put("19877232369", "何翠柔");
        contacts.put("18706287692", "钱溶艳");
        contacts.put("18944239644", "钱琳");
        contacts.put("17325302007", "缪静欣");
        contacts.put("15050109398", "焦秋菊");
        contacts.put("19879419704", "吕访琴");
        contacts.put("16480981069", "沈丹");
        contacts.put("18674257265", "褚美丽");
        contacts.put("18302820904", "孙怡");
        contacts.put("15133295266", "许婵");
        contacts.put("17868457605", "曹红恋");
        contacts.put("15490732767", "吕柔");
        contacts.put("15064972307", "冯怜云");
    }

    //获取1条数据，写出6条数据（2个联系人和三个时间维度全排列）
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //获取rowKey并切割
        String rowKey = Bytes.toString(key.get());
        String[] split = rowKey.split("_");

        //针对f2列族数据不做操作
        if ("0".equals(split[5])) {
            return;
        }

        //取出相应字段
        String caller = split[1];//主叫
        String buildTime = split[2];//通话建立时间
        String callee = split[4];//被叫
        String duration = split[6];//通话时长

        //2017-05-23 12:11:11
        String year = buildTime.substring(0, 4);
        String month = buildTime.substring(5, 7);
        String day = buildTime.substring(8, 10);

        //设置值（通话时长）
        v.set(duration);

        //创建三个时间维度
        DateDimension yearDimension = new DateDimension(year, "-1", "-1");//年维度
        DateDimension monthDimension = new DateDimension(year, month, "-1");//月维度
        DateDimension dayDimension = new DateDimension(year, month, day);//日维度

        //主叫联系人维度
        contactDimension.setPhoneNum(caller);
        contactDimension.setName(contacts.get(caller));
        commDimension.setContactDimension(contactDimension);

        //年维度
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension, v);

        //月维度
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension, v);

        //日维度
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension, v);

        //被叫联系人维度
        contactDimension.setPhoneNum(callee);
        contactDimension.setName(contacts.get(callee));
        commDimension.setContactDimension(contactDimension);

        //年维度
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension, v);

        //月维度
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension, v);

        //日维度
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension, v);
    }
}