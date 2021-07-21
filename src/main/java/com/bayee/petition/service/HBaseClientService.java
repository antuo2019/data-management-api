package com.bayee.petition.service;

import java.util.Map;

public interface HBaseClientService {

    /**
     * 表的总条数
     * @param tableName 表名
     * @return
     */
    Map<String, Object> getCount(String tableName);

    /**
     * 全量查询
     * @param tableName 表名
     * @param fields 展示的字段
     * @return
     */
    Map<String, Object> getAllData(String tableName, String fields);

    /**
     * 分页查询
     * @param tableName 表名
     * @param fields 展示的字段
     * @param pageIndex 页码
     * @param pageSize 每页展示的条数
     * @return
     */
    Map<String, Object> getPageData(String tableName, String fields, int pageIndex, int pageSize);

    /**
     * 全量查询并排序
     * @param tableName 表名
     * @param fields 展示的字段
     * @return
     */
    Map<String, Object> getAllSortData(String tableName, String fields, String sort);

    /**
     * 分页查询并排序
     * @param tableName 表名
     * @param fields 展示的字段
     * @param pageIndex 页码
     * @param pageSize 每页展示的条数
     * @return
     */
    Map<String, Object> getPageSortData(String tableName, String fields, String sort, int pageIndex, int pageSize);

    /**
     * 全量条件查询
     * @param tableName 表名
     * @param fields 展示的字段
     * @param symbol 运算符
     * @param condition 条件
     * @return
     */
    Map<String, Object> getAllConditionData(String tableName, String fields, String symbol, String condition);

    /**
     * 分页条件查询
     * @param tableName 表名
     * @param fields 展示的字段
     * @param symbol 运算符
     * @param condition 条件
     * @param pageIndex 页码
     * @param pageSize 每页展示的条数
     * @return
     */
    Map<String, Object> getPageConditionData(String tableName, String fields, String symbol, String condition, int pageIndex, int pageSize);

    Map<String, Object> getPageConditionSortData(String tableName, String fields, String symbol, String condition, String sort, int pageIndex, int pageSize);

    /**
     * 全量条件查询
     * @param tableName 表名
     * @param fields 展示的字段
     * @param symbol 运算符
     * @param condition 条件
     * @return
     */
    Map<String, Object> getAllConditionSortData(String tableName, String fields, String symbol, String condition, String sort);

}
