package com.bayee.petition.service;

import com.bayee.petition.domain.HbCatlogDAO;

import java.util.List;

/**
 * @Description: TODO
 * @Author YueCang
 * @Date 2021/1/2711:16
 */
public interface HbCatlogService {

    // 当前任务
    List<HbCatlogDAO> getCurrentJob();

    // 数据采集趋势
    List<HbCatlogDAO> getDataCollectionTrend();

    // 数据量Top5
    List<HbCatlogDAO> getDataTop5();

    // 数据种类
    List<HbCatlogDAO> getDataCategory();

    // 数据来源
    List<HbCatlogDAO> getDataSources();

    // 总数
    int count(HbCatlogDAO hbCatlogDAO);

    // 分页查询
    List<HbCatlogDAO> findAllNature(HbCatlogDAO hbCatlogDAO);
    
    List<String> selectCategory();
    
    List<String> selectOrigin();
    
    HbCatlogDAO findById(Integer id);

}
