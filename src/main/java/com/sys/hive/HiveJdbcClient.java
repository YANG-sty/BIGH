package com.sys.hive;


import java.sql.*;

/**
 * 服务端使用 hiveserver2 启动服务
 *
 * Create by yang_zzu on 2020/9/11 on 10:38
 */
public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.232.100:10000/default", "root", "");
        Statement stmt = conn.createStatement();
        String sql = "select * from psn limit 5 ";

        try {
            ResultSet res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "-" + res.getString("name"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }

    }
}
