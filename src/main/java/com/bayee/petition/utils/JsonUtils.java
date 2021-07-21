package com.bayee.petition.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import java.util.*;

public class JsonUtils {

    public final static List<String> STATION_CODE = Arrays.asList("3160211001","3160211002","3160211003","3160211004","3160211005",
            "3160211006","3160211007","3160211008","3160211009","3160211010","3160211011","3160211012",
            "3160211013","3160211014","3160211015","3160211016","3160211017","3160211018","3160211019",
            "3160211020","3160211021","3160211022","3160211023","3160211024","3160211025","3160211026",
            "3160211027","3160211028","3160211029","3160211030","3160211031","3160211032","3160211033",
            "3160211034","3160211035","3160211036","3160211037","3160211038","3160211039","3160211040",
            "3160211041","3160211042","3160211043","3160211044","3160211045","3160211046","3160211047",
            "3160211048","2768","2769","2770","3773","3921","3979","4171","4257","4393","4477","5003",
            "5947","5948","5949","6844","6845","6858","7312","7328","7329","7648","8101","13321");

    public static String processMessage(String message){
        JSONObject jsonObject = JSON.parseObject(message);
        message = jsonObject.getString("message");
        jsonObject = JSON.parseObject(message);
        String rows = jsonObject.getString("rows");
        rows = rows.substring(1, rows.lastIndexOf("]"));
        return rows;
    }

    public static String getTotal(String message){
        JSONObject jsonObject = JSONObject.parseObject(message);
        message = jsonObject.getString("message");
        jsonObject = JSONObject.parseObject(message);
        String total = jsonObject.getString("total");
        return total;
    }

    public static String getTotal2(String message){
        JSONObject jsonObject = JSON.parseObject(message);
        String total = jsonObject.getString("total");
        return total;
    }

    /**
     * 将接口返回的json进行处理
     * @param message 将result数组返回
     * @return
     */
    public static String processMessage2(String message){
        JSONObject jsonObject = JSON.parseObject(message);
        message = jsonObject.getString("message");
        if (message.equals("[]")) {
            message = null;
        } else {
            message = message.substring(1, message.lastIndexOf("]"));
        }
        return message;
    }

    /**
     * 将接口返回的json进行处理
     * @param message 将result数组返回
     * @return
     */
    public static String processMessage3(String message){
        JSONObject jsonObject = JSON.parseObject(message);
        message = jsonObject.getString("data");
        if (message.equals("[]")) {
            message = null;
        } else {
            message = message.substring(1, message.lastIndexOf("]"));
        }
        return message;
    }

}
