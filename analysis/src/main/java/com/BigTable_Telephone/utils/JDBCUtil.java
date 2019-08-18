package com.BigTable_Telephone.utils;


import java.sql.*;

public class JDBCUtil {

    private static Connection connection = null;

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://master:3306/ct?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "39237734";

    // 获取JDBC连接
    private static Connection getConnection() {
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            return DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet) {
        // 连接
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        // 预编译
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        // 结果集
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 单例的JDBC连接
    public static Connection getInstance() {
        if (connection == null){
            connection = getConnection();
        }
        return connection;
    }

}
