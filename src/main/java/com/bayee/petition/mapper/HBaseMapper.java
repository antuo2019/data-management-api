package com.bayee.petition.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.HashMap;
import java.util.List;

/**
 * @author antuo
 * @since 2021/7/15 14:27
 */

public interface HBaseMapper {
    @Select("select count(1) from ${tableName}")
    long queryCount(@Param(value = "tableName") String tableName);

    @Select("select count(1) from ${tableName} where ${condition}")
    long queryCountCondition(@Param(value = "tableName") String tableName,@Param(value = "condition") String condition);

    @Select("select ${fields} from ${tableName}")
    List<HashMap<String,Object>> queryAll(@Param(value = "tableName") String tableName, @Param(value = "fields") String fields);

    @Select("select ${fields} from ${tableName} order by ${sort}")
    List<HashMap<String,Object>> queryAllSort(@Param(value = "tableName") String tableName, @Param(value = "fields") String fields, @Param(value = "sort") String sort);

    @Select("select ${fields} from ${tableName} where ${condition}")
    List<HashMap<String,Object>> queryAllCondition(@Param(value = "tableName") String tableName, @Param(value = "fields") String fields, @Param(value = "condition") String condition);

    @Select("select ${fields} from ${tableName} where ${condition} order by ${sort}")
    List<HashMap<String,Object>> queryAllConditionSort(@Param(value = "tableName") String tableName, @Param(value = "fields") String fields, @Param(value = "condition") String condition, @Param(value = "sort") String sort);

    @Select("select ${fields} from ${tableName} limit ${pageSize} offset ${pageIndex}")
    List<HashMap<String,Object>> queryPage(@Param(value = "tableName") String tableName, @Param(value = "fields") String fields, @Param(value = "pageIndex") int pageIndex, @Param(value = "pageSize") int pageSize);

    @Select("select ${fields} from ${tableName} order by ${sort} limit ${pageSize} offset ${pageIndex}")
    List<HashMap<String,Object>> queryPageSort(@Param(value = "tableName") String tableName, @Param(value = "fields") String fields, @Param(value = "sort") String sort, @Param(value = "pageIndex") int pageIndex, @Param(value = "pageSize") int pageSize);

    @Select("select ${fields} from ${tableName} where ${condition} limit ${pageSize} offset ${pageIndex}")
    List<HashMap<String,Object>> queryPageCondition(@Param(value = "tableName") String tableName, @Param(value = "fields") String fields, @Param(value = "condition") String condition, @Param(value = "pageIndex") int pageIndex, @Param(value = "pageSize") int pageSize);

    @Select("select ${fields} from ${tableName} where ${condition} order by ${sort} limit ${pageSize} offset ${pageIndex}")
    List<HashMap<String,Object>> queryPageConditionSort(@Param(value = "tableName") String tableName, @Param(value = "fields") String fields, @Param(value = "condition") String condition, @Param(value = "sort") String sort, @Param(value = "pageIndex") int pageIndex, @Param(value = "pageSize") int pageSize);
}
