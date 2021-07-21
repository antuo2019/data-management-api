package com.bayee.petition.utils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class JDBCUtils {
    //创建Properties集合
    private static Properties p = new Properties();
    static {
        //通过流写入配置文件中的数据
        InputStream is = JDBCUtils.class.getResourceAsStream("/config.properties");
        try {
            p.load(is);
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {is.close();}catch(Exception e1){}
        }
    }
    public static Connection getConnection() throws Exception{
        Connection conn = null;
        if(conn == null) {
            Class.forName(p.getProperty("phoenix.driver"));
            conn = DriverManager.getConnection(p.getProperty("phoenix.url"));
        }
        return conn;
    }
    public static void close(Connection conn, Statement state, ResultSet rs) {
        if(rs != null) {try{rs.close();}catch(Exception e){}}
        if(state != null) {try{state.close();}catch(Exception e){}}
        if(conn != null) {try{conn.close();}catch(Exception e){}}
    }

    public static String quotes(String filed) {
        return "\""+filed+"\"";
    }
}

