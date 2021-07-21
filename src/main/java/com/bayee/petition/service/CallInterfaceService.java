package com.bayee.petition.service;


/**
 * 调用接口服务
 */
public interface CallInterfaceService {

    /*-------------------------------------------- 易航海相关接口开始 ------------------------------------------------*/

    /**
     * getAisMap 获取区域AIS绿点绿点数据
     * @param minLon 最小经度
     * @param maxLon 最大经度
     * @param minLat 最小纬度
     * @param maxLat 最大纬度
     * @param precision 显示等级1-12 数字越大越密集
     * @return
     */
    String getAisMap(String minLon, String maxLon, String minLat, String maxLat, String precision);

    /**
     * shipListData 获取区域AIS概要数据
     * @param minLon 最小经度
     * @param maxLon 最大经度
     * @param minLat 最小纬度
     * @param maxLat 最大纬度
     * @return
     */
    String shipListData(String minLon, String maxLon, String minLat, String maxLat);

    /**
     * getShipInfo 获取AIS详情数据 实时数据
     * @param mmsi MMSI
     * @return
     */
    String getShipInfo(String mmsi);

    /**
     * searchShip AIS模糊查询
     * @param keyWord 关键字
     * @return
     */
    String searchShip(String keyWord);

    /**
     * shipTrajectoryByHbase 获取AIS某时间段轨迹
     * @param mmsi MMSI
     * @param startTime 开始时间戳
     * @param endTime 结束时间戳
     * @return
     */
    //获取不到数据
    String shipTrajectoryByHbase(String mmsi, String startTime, String endTime);

    /**
     * getMyPorts 获取已绑定港口数据
     * @return
     */
    String getMyPorts();

    /**
     * getTideByPort 获取某个港口的7天潮汐数据
     * @param code 港口ID
     * @return
     */
    String getTideByPort(String code);

    /**
     * findComplexByYearOrName 根据年份或者名字查询台风
     * @param year 年份
     * @param name 台风名字
     * @return
     */
    String findComplexByYearOrName(String year, String name);

    /**
     * findComplexNow 获取当前实时的台风信息
     * @return
     */
    String findComplexNow();

    /**
     * findComplexByComplexId 根据台风ID查询台风详情
     * @param id 台风ID
     * @return
     */
    String findComplexByComplexId(String id);

    /**
     * harbourType 获取已绑定港口类型数据
     * @return
     */
    String harbourType();

    /**
     * harbour 获取某个类型港口数据
     * @param harbourType 港口类型
     * @return
     */
    String harbour(String harbourType);

    /**
     * orderLine 获取定线制绕行基础数据数据
     * @return
     */
    String orderLine();

    /**
     * find 获取两个港口之间的航线数据
     * @param startCode 开始港口编号
     * @param startName 开始港口名称
     * @param endCode 目的港口编号
     * @param endName 目的港口名称
     * @param shipHeight 船高
     * @param depth 吃水
     * @param shipton 吨位
     * @param laneName 定制绕行线名称，非必要参数
     * @return
     */
    String find(String startCode, String startName, String endCode, String endName, String shipHeight, String depth,
                String shipton, String laneName);

    /**
     * getNavigation 获取所有维护过的有效航警信息
     * @param code 港口ID
     * @param lastGetTime 上次请求的时间，非必要参数
     * @return
     */
    String getNavigation(String code, String lastGetTime);

    /** 通过elasticSearchId获取航警详情
     * getNavigationInfoEsId
     * @param esId elasticSearchId
     * @return
     */
    String getNavigationInfoEsId(String esId);

    /**
     * findComplexYear 查询台风年份
     * @return
     */
    String findComplexYear();

    /**
     * findComplexByInfo 根据台风编号查询台风详情
     * @param tfbh 台风编号
     * @return
     */
    String findComplexByInfo(String tfbh);

    /**
     * findComplexByCitys 根据台风编号查询台风可能登陆的城市
     * @param tfbh 台风编号
     * @return
     */
    String findComplexByCitys(String tfbh);

    /**
     * getWindAreaDataList 获取大风分布
     * @return
     */
    String getWindAreaDataList();

    /**
     * getWindAreaDetailInfo 获取大风详情
     * @param url 从上个接口获取的json文件url
     * @return
     */
    String getWindAreaDetailInfo(String url);

    /**
     * getManualForecastByPaged 获取服务材料信息
     * @param pageNum 显示页码
     * @param pageSize 一页展示多少条数据
     * @return
     */
    String getManualForecastByPaged(String pageNum, String pageSize);

    /**
     * getManualForecastById 获取服务材料详情
     * @param id 上个接口返回的材料id
     * @return
     */
    String getManualForecastById(String id);

    /**
     * cx 获取港口潮汐信息
     * @param ids 所查地区拼接id
     * @return
     */
    String cx(String ids);

    /**
     * getShipInfoListByMMSIFromHbase 批量获取船舶简要信息
     * @param mmsis MMSI，多个按逗号分割区别
     * @return
     */
    String getShipInfoListByMMSIFromHbase(String mmsis);

    /**
     * getAllPubNameByType 航官方发布根据类型获取所有的发布单位
     * @param type 1:通告  2：警告 3：海事公告
     * @return
     */
    String getAllPubNameByType(String type);

    /**
     * getNavigationsInfosByPage 分页获取海事信息
     * @param type 1:通告  2：警告 3：海事公告
     * @param pageName 海事名称
     * @return
     */
    String getNavigationsInfosByPage(String type, String pageName);

    /*-------------------------------------------- 易航海相关接口结束 ------------------------------------------------*/

}
