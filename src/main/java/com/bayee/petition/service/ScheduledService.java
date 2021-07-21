package com.bayee.petition.service;

import com.bayee.petition.HBaseClient;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.Map;
@Component
@EnableScheduling
public class ScheduledService {

    @Scheduled(cron = "0 0 0 1/1 * ?")
    public void execute() {
        Connection conn = null;
        PreparedStatement pstm = null;
        PreparedStatement pstm1 = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://10.25.18.217:3301/zssjzx_management?autoReconnect=true&characterEncoding=UTF-8&failOverReadOnly=false&useUnicode=true&rewriteBatchedStatements=TRUE","root","zssjzx2021");
            String querySql = "select h_tb_name,`count` from hb_catlog";
            pstm = conn.prepareStatement(querySql);
            rs = pstm.executeQuery();
            String updateSql = "update hb_catlog set `count`=?,update_time=?,increment_count=? where h_tb_name=?";
            pstm1 = conn.prepareStatement(updateSql);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Map<String, Object> map = null;
            while (rs.next()) {
                String tableName = rs.getString(1);
                int count = rs.getInt(2);
                if (!"hb_hb_catlog".equals(tableName)) {
                    map = HBaseClient.queryCount(tableName);
                    Object count1 = map.get("count");
                    int incrementCount = Integer.parseInt(String.valueOf(count1)) - count;
                    pstm1.setInt(1, Integer.parseInt(String.valueOf(count1)));
                    pstm1.setTimestamp(2, timestamp);
                    pstm1.setInt(3, incrementCount);
                    pstm1.setString(4, tableName);
                    pstm1.executeUpdate();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstm1 != null) {
                    pstm1.close();
                }
            } catch (Exception e) {e.printStackTrace();}

            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e) {e.printStackTrace();}
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {e.printStackTrace();}
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {e.printStackTrace();}
        }
    }
}
