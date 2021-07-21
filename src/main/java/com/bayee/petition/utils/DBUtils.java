package com.bayee.petition.utils;

import com.bayee.bean.ManagerTableInfoBean;
import org.apache.log4j.Logger;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DBUtils {

    private static final Logger log = Logger.getLogger(DBUtils.class);

    //获取Connection连接对象的方法,使用static方便之后在其他类中调用
    public static Connection getConn(String driver, String url, String username, String password) {
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url,username,password);
            log.info("Create connected successfully");
        } catch (ClassNotFoundException e) {
            log.info("Create connected failed");
            e.printStackTrace();
        } catch (SQLException e) {
            log.info("Create connected failed");
            e.printStackTrace();
        }
        return conn;
    }

    //关闭资源的方法
    public  static void close(ResultSet rs, PreparedStatement ps, Connection conn) {
        if(rs!=null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(ps!=null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(conn!=null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public static List<ManagerTableInfoBean> tableInfo(String originSystem, String driver, String url, String user, String pwd, String dbName){

        List<ManagerTableInfoBean> tableInfoBeans = null;
        url += "/" + dbName;
        System.out.println(url);
        Connection conn= getConn(driver, url, user, pwd);

        String scanTableSql = String.format("select table_name,table_comment from information_schema.tables where table_schema='" + dbName +"'");

        try {
            PreparedStatement ps = conn.prepareStatement(scanTableSql);
            ResultSet rs = ps.executeQuery();
            tableInfoBeans = new ArrayList();
            while (rs.next()){
                if(!rs.getString("table_comment").isEmpty() && !rs.getString("table_comment").equals("企业隐患信息附件管理")){
                    String tableName = rs.getString("table_name");
                    String tableNameCh = rs.getString("table_comment");

                    ManagerTableInfoBean.Builder managerTableBuilder = new ManagerTableInfoBean.Builder();
                    managerTableBuilder.originSystem(originSystem).tableName("hb_"+ tableName).tableNameCh(tableNameCh).injectionType(2);

                    if(tableName.equals("sys_user采集同步后") || tableName.equals("yjsj_sq书记同步前")){
                        continue;
                    }
                    String tableCountSql = String.format("select count(1) count from %s",tableName);;
                    System.out.println("######## "+tableCountSql);

                    PreparedStatement tableCountPs = conn.prepareStatement(tableCountSql);
                    ResultSet tableCountRs = tableCountPs.executeQuery();
                    if(tableCountRs.next()) {
                        managerTableBuilder.count(tableCountRs.getInt("count"));
                    }

                    String tableSchemaSql = String.format(
                            "select column_name,column_comment from information_schema.columns where table_schema='"
                                    + dbName
                                    +"' and table_name = '"
                                    + tableName
                                    + "'"
                    );

                    PreparedStatement tableSchemaPs = conn.prepareStatement(tableSchemaSql);
                    ResultSet tableSchemaRs = tableSchemaPs.executeQuery();
                    StringBuilder fields = new StringBuilder();
                    StringBuilder fieldsCh = new StringBuilder();
                    while (tableSchemaRs.next()){
                        if (!tableSchemaRs.getString("column_comment").isEmpty()){
                            fields.append(tableSchemaRs.getString("column_name") + ",");
                            fieldsCh.append(tableSchemaRs.getString("column_comment") + ",");
                        }
                    }

                    if(fields.length() != 0){
                        managerTableBuilder.fields(fields.substring(0,fields.length()-1)).fieldsCh(fieldsCh.substring(0,fieldsCh.length()-1));
                    }

                    managerTableBuilder.creationDate(getCurrentTime());

                    tableInfoBeans.add(managerTableBuilder.build());
                }
            }

            DBUtils.close(rs,ps,conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableInfoBeans;
    }

    public static String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(System.currentTimeMillis()));
    }

    public static void insertTableInfo(List<ManagerTableInfoBean> tableInfoBeanList){
        String dbName =  PropertyUtils.getValue("hbase.manager.db.name");
        String mysqlDriver =  PropertyUtils.getValue("mysql.jdbc.driver");
        String managerUrl =  PropertyUtils.getValue("hbase.manager.url") + "/" + dbName;
        String managerUser =  PropertyUtils.getValue("hbase.manager.user");
        String managerPwd =  PropertyUtils.getValue("hbase.manager.pwd");

        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = getConn(mysqlDriver, managerUrl, managerUser, managerPwd);

            for (ManagerTableInfoBean bean : tableInfoBeanList) {
                String insertSql = String.format("insert into hb_catlog(" +
                        "h_tb_name,h_tb_name_ch,origin_system,injection_type,fields_en,fields_ch,creation_date,count)" +
                        "VALUES('%s','%s','%s',%s,'%s','%s','%s',%s)",bean.getTableName(),bean.getTableNameCh(),bean.getOriginSystem(),
                        bean.getInjectionType(),bean.getFields(),bean.getFieldsCh(),bean.getCreationDate(),bean.getCount());
                ps = conn.prepareStatement(insertSql);
                ps.execute();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        DBUtils.close(null, ps, conn);
    }


    public static void main(String[] args) throws SQLException {
        String dbName =  "sjdc";
        String driver = "com.mysql.jdbc.Driver";
        String url =  "jdbc:mysql://10.80.18.65:3301";
        String user =  "root";
        String password =  "dahuacloud";

        String originSystem = "海上安监主题库";

        ArrayList<ManagerTableInfoBean> newBeans = new ArrayList<>();

        List<ManagerTableInfoBean> tableInfoBeans = tableInfo(originSystem,driver, url, user, password, dbName);
        System.out.println("### 表的数量：" + tableInfoBeans.size());

        for (ManagerTableInfoBean bean : tableInfoBeans) {
            System.out.println(bean);
        }

//        for (ManagerTableInfoBean newBean : newBeans) {
//            System.out.println(newBean);
//        }

        insertTableInfo(tableInfoBeans);
    }
}
