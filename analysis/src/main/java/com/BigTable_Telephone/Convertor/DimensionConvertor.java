package com.BigTable_Telephone.Convertor;

import com.BigTable_Telephone.kv.ContactDimension;
import com.BigTable_Telephone.kv.DateDimension;
import com.BigTable_Telephone.kv.base.BaseDimension;
import com.BigTable_Telephone.utils.JDBCUtil;
import com.BigTable_Telephone.utils.LRUCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//获取联系人&时间维度ID
public class DimensionConvertor {

    private LRUCache keyCache = new LRUCache(1000);

    public int getDimensionID(BaseDimension baseDimension) throws SQLException {

        String dimensionKey = getDimensionKey(baseDimension);
        //1.获取缓存中的值
        if (keyCache.containsKey(dimensionKey)) {
            return keyCache.get(dimensionKey);
        }

        //获取JDBC连接
        Connection connection = JDBCUtil.getInstance();

        //获取sql语句（2条，查询，插入）
        String[] sqls = getSqls(baseDimension);

        //2.查询mysql表相应维度是否有值
        //3.如果查询结果为空，执行插入数据操作
        //4.再次执行查询
        int id = execSql(connection, sqls);
        if (id == 0) throw new RuntimeException("未找到匹配维度!");

        //5.将结果写入缓存
        keyCache.put(dimensionKey, id);

        //6.将查询结果返回
        return id;
    }

    private synchronized int execSql(Connection connection, String[] sqls) throws SQLException {

        //第一次查询
        PreparedStatement preparedStatement = connection.prepareStatement(sqls[0]);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        //插入操作
        preparedStatement = connection.prepareStatement(sqls[1]);
        preparedStatement.executeUpdate();
        //第二次查询
        preparedStatement = connection.prepareStatement(sqls[0]);
        resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return 0;
    }

    //根据相应的维度信息生成2条sql语句
    private String[] getSqls(BaseDimension baseDimension) {
        String[] sqls = new String[2];
        if (baseDimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            String phoneNum = contactDimension.getPhoneNum();
            String name = contactDimension.getName();
            //查询操作语句
            sqls[0] = "SELECT `id` FROM `tb_contacts` WHERE `telephone`=" + "'" + phoneNum + "'";
            //插入操作语句
            sqls[1] = "INSERT INTO `tb_contacts` VALUES (NULL," + "'" + phoneNum + "','" + name + "')";
        } else {
            DateDimension dateDimension = (DateDimension) baseDimension;
            int year = Integer.valueOf(dateDimension.getYear());
            int month = Integer.valueOf(dateDimension.getMonth());
            int day = Integer.valueOf(dateDimension.getDay());

            //查询操作语句
            sqls[0] = "SELECT `id` FROM `tb_dimension_date` WHERE `year`=" + year + " AND `month` =" + month + " AND `day` =" + day;
            //插入操作语句
            sqls[1] = "INSERT INTO `tb_dimension_date` VALUES (NULL," + year + "," + month + "," + day + ")";
        }
        return sqls;
    }

    //根据维度信息获取维度描述
    private String getDimensionKey(BaseDimension baseDimension) {
        StringBuffer sb = new StringBuffer();
        if (baseDimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            sb.append(contactDimension.getPhoneNum());
        } else {
            DateDimension dateDimension = (DateDimension) baseDimension;
            sb.append(dateDimension.getYear()).append(dateDimension.getMonth()).append(dateDimension.getDay());
        }
        return sb.toString();
    }


}
