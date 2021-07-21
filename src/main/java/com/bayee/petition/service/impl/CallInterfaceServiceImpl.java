package com.bayee.petition.service.impl;

import com.bayee.petition.service.CallInterfaceService;
import com.bayee.petition.utils.*;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @ClassName CallInterfaceServiceImpl
 * @Description: TODO
 * @Author Lyb
 * @Date 2021/3/11
 **/

@Service
public class CallInterfaceServiceImpl implements CallInterfaceService {

    private static final Logger log = Logger.getLogger(CallInterfaceServiceImpl.class);

    private static final String BASE_URL = PropertyUtils.getValue("yhh.interface.base.url");
    private static final String APPKEY = PropertyUtils.getValue("yhh.app.key");
    private static final String APPSRCRET = PropertyUtils.getValue("yhh.app.secret");
    private static final String WATER_URL = PropertyUtils.getValue("yhh.interface.water.url");

    private Map<String,String> map = null;

    @Override
    public String getAisMap(String minLon, String maxLon, String minLat, String maxLat, String precision) {

        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("minLon", minLon);
        map.put("maxLon", maxLon);
        map.put("minLat", minLat);
        map.put("maxLat", maxLat);
        map.put("precision", precision);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/ship/getAisMap";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String shipListData(String minLon, String maxLon, String minLat, String maxLat) {

        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("minLon", minLon);
        map.put("maxLon", maxLon);
        map.put("minLat", minLat);
        map.put("maxLat", maxLat);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/ship/shipListData";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String getShipInfo(String mmsi) {

        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("mmsi", mmsi);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/ship/getShipInfo";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String searchShip(String keyWord) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("keyWord", keyWord);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/ship/searchShip";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String shipTrajectoryByHbase(String mmsi, String startTime, String endTime) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("mmsi", mmsi);
        map.put("startTime", startTime);
        map.put("endTime",endTime);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/ship/shipTrajectoryByHbase";

        return HttpClientUtil.doGet(url,map);
    }

    @Override
    public String getMyPorts() {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/getMyPorts";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String getTideByPort(String code) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("code",code);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/getTideByPort";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String findComplexByYearOrName(String year, String name) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("year",year);
        map.put("name",name);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/findComplexByYearOrName";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String findComplexNow() {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/findComplexNow";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String findComplexByComplexId(String id) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("id",id);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/findComplexByComplexId";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String harbourType() {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/route/harbourType";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String harbour(String harbourType) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("harbourType",harbourType);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/route/harbour";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String orderLine() {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/route/orderLine";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String find(String startCode, String startName, String endCode, String endName, String shipHeight, String depth, String shipton, String laneName) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("startCode",startCode);
        map.put("startName",startName);
        map.put("endCode",endCode);
        map.put("endName",endName);
        map.put("shipHeight",shipHeight);
        map.put("depth",depth);
        map.put("shipton",shipton);
        map.put("laneName",laneName);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/route/find";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String getNavigation(String code, String lastGetTime) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("code",code);
        map.put("lastGetTime", lastGetTime);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);
        String url = BASE_URL + "/weather/getNavigation";
        return HttpClientUtil.doPost(url,map);
    }


    @Override
    public String getNavigationInfoEsId(String esId) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("esId",esId);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/getNavigationInfoEsId";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String findComplexYear() {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/findComplexYear";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String findComplexByInfo(String tfbh) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("tfbh",tfbh);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/findComplexByInfo";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String findComplexByCitys(String tfbh) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("tfbh",tfbh);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/findComplexByCitys";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String getWindAreaDataList() {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/getWindAreaDataList";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String getWindAreaDetailInfo(String url) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("url",url);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String baseUrl = BASE_URL + "/weather/getWindAreaDetailInfo";

        return HttpClientUtil.doPost(baseUrl,map);
    }

    @Override
    public String getManualForecastByPaged(String pageNum, String pageSize) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("pageNum",pageNum);
        map.put("pageSize",pageSize);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/getManualForecastByPaged";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String getManualForecastById(String id) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("id",id);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/getManualForecastById";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String cx(String ids) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("ids",ids);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/alllPort/cx";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String getShipInfoListByMMSIFromHbase(String mmsis) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("mmsis",mmsis);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/ship/getShipInfoListByMMSIFromHbase";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String getAllPubNameByType(String type) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("type",type);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/getAllPubNameByType";

        return HttpClientUtil.doPost(url,map);
    }

    @Override
    public String getNavigationsInfosByPage(String type, String pageName) {
        map = new HashMap<>();
        map.put("appkey", APPKEY);
        map.put("appSecret",APPSRCRET);
        map.put("type",type);
        map.put("pageName",pageName);
        String timestamp = System.currentTimeMillis() + "";
        map.put("timestamp", timestamp);

        String url = BASE_URL + "/weather/getNavigationsInfosByPage";

        return HttpClientUtil.doPost(url,map);
    }
}
