package com.bayee.petition.service.impl;

import com.bayee.petition.HBaseClient;
import com.bayee.petition.mapper.HBaseMapper;
import com.bayee.petition.service.HBaseClientService;
import com.bayee.petition.utils.JDBCUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Service
public class HBaseClientServiceImpl implements HBaseClientService {

    @Autowired
    private HBaseMapper hBaseMapper;

    @Override
    public Map<String, Object> getCount(String tableName) {
        Map<String, Object> map = new HashMap<>();
        tableName =quotes(tableName);
        map.put("count",hBaseMapper.queryCount(tableName));
        return map;
    }

    @Override
    public Map<String, Object> getAllData(String tableName, String fields) {
        Map<String, Object> map = new TreeMap<>();
        tableName =quotes(tableName);
        if(!"*".equals(fields)) {
            fields = getField(fields);
        }
        List<HashMap<String, Object>> data = hBaseMapper.queryAll(tableName, fields);
        map.put("count",data.size());
        map.put("data",data);
        return map;
    }

    @Override
    public Map<String, Object> getPageData(String tableName, String fields, int pageIndex, int pageSize) {
        Map<String, Object> map = new TreeMap<>();
        tableName =quotes(tableName);
        if(!"*".equals(fields)) {
            fields = getField(fields);
        }
        pageIndex = (pageIndex - 1)*pageSize;
        map.put("count",hBaseMapper.queryCount(tableName));
        map.put("data",hBaseMapper.queryPage(tableName,fields,pageIndex,pageSize));
        return map;
    }

    @Override
    public Map<String, Object> getAllSortData(String tableName, String fields, String sort) {
        Map<String, Object> map = new TreeMap<>();
        tableName =quotes(tableName);
        if(!"*".equals(fields)) {
            fields = getField(fields);
        }
        sort = getSort(sort);
        List<HashMap<String, Object>> data = hBaseMapper.queryAllSort(tableName, fields, sort);
        map.put("count",data.size());
        map.put("data",data);
        return map;
    }

    @Override
    public Map<String, Object> getPageSortData(String tableName, String fields, String sort, int pageIndex, int pageSize) {
        Map<String, Object> map = new TreeMap<>();
        tableName =quotes(tableName);
        if(!"*".equals(fields)) {
            fields = getField(fields);
        }
        sort = getSort(sort);
        pageIndex = (pageIndex - 1)*pageSize;
        map.put("count",hBaseMapper.queryCount(tableName));
        map.put("data",hBaseMapper.queryPageSort(tableName,fields,sort,pageIndex,pageSize));
        return map;
    }

    @Override
    public Map<String, Object> getAllConditionData(String tableName, String fields, String symbol, String condition) {
        Map<String, Object> map = new TreeMap<>();
        tableName =quotes(tableName);
        if(!"*".equals(fields)) {
            fields = getField(fields);
        }
        condition = getFactor(symbol,condition);

        List<HashMap<String, Object>> data = hBaseMapper.queryAllCondition(tableName, fields, condition);
        map.put("count",data.size());
        map.put("data",data);
        return map;
    }

    @Override
    public Map<String, Object> getPageConditionData(String tableName, String fields, String symbol, String condition, int pageIndex, int pageSize) {
        Map<String, Object> map = new TreeMap<>();
        tableName =quotes(tableName);
        if(!"*".equals(fields)) {
            fields = getField(fields);
        }
        condition = getFactor(symbol,condition);
        pageIndex = (pageIndex - 1)*pageSize;
        map.put("count",hBaseMapper.queryCountCondition(tableName,condition));
        map.put("data",hBaseMapper.queryPageCondition(tableName,fields,condition,pageIndex,pageSize));
        return map;
    }

    @Override
    public Map<String, Object> getPageConditionSortData(String tableName, String fields, String symbol, String condition, String sort, int pageIndex, int pageSize) {
        Map<String, Object> map = new TreeMap<>();
        tableName =quotes(tableName);
        if(!"*".equals(fields)) {
            fields = getField(fields);
        }
        condition = getFactor(symbol,condition);
        sort = getSort(sort);
        pageIndex = (pageIndex - 1)*pageSize;
        map.put("count",hBaseMapper.queryCountCondition(tableName,condition));
        map.put("data",hBaseMapper.queryPageConditionSort(tableName,fields,condition,sort,pageIndex,pageSize));
        return map;
    }

    @Override
    public Map<String, Object> getAllConditionSortData(String tableName, String fields, String symbol, String condition, String sort) {
        Map<String, Object> map = new TreeMap<>();
        tableName =quotes(tableName);
        if(!"*".equals(fields)) {
            fields = getField(fields);
        }
        condition = getFactor(symbol,condition);
        sort = getSort(sort);
        List<HashMap<String, Object>> data = hBaseMapper.queryAllConditionSort(tableName, fields, condition, sort);
        map.put("count",data.size());
        map.put("data",data);
        return map;
    }

    private String quotes(String filed) {
        return "\""+filed+"\"";
    }

    private String getField(String fields) {
        String[] fieldArray = fields.split(",");
        String field = "";
        for (int i = 0; i < fieldArray.length; i++) {
            fieldArray[i] = quotes(fieldArray[i]);
            field = field + fieldArray[i] + ",";
        }
        field = field.substring(0, field.lastIndexOf(","));
        return field;
    }

    private  String getSort(String sort) {
        String term = "";
        //以逗号区分多个字段排序
        String[] sorts = sort.split(",");
        for (String s : sorts) {
            //以小数点将条件拆分为字段 排序规则
            String[] terms = s.split("\\.");
            String temp = "";
            terms[0] = JDBCUtils.quotes(terms[0]);
            for (int i = 0; i < terms.length; i++) {
                temp = temp + terms[i] + " ";
            }
            term = term + temp.trim() + ",";
        }
        return term.substring(0,term.lastIndexOf(",")).trim();
    }

    private String getFactor(String symbol, String condition) {
        String term = "";
        if(condition != null) {
            //以逗号区分多个条件
            String[] conditions = condition.split(",");
            for (String s : conditions) {
                //以小数点将条件拆分为字段 条件 值
                String[] terms = s.split("\\.", 3);
                String temp = "";
                if ("like".equals(terms[1])) {
                    terms[0] = JDBCUtils.quotes(terms[0]);
                    terms[2] = "'%"+terms[2]+"%'";
                } else {
                    //判断值是否是数字类型
                    if (NumberUtils.isNumber(terms[2])) {
                        terms[0] = "to_number(" + JDBCUtils.quotes(terms[0]) + ")";
                    } else {
                        terms[0] = JDBCUtils.quotes(terms[0]);
                        if (!"null".equals(terms[2])) {
                            terms[2] = "'" + terms[2] + "'";
                        }
                    }
                }
                for (int i = 0; i < terms.length; i++) {
                    temp = temp + terms[i] + " ";
                }
                term = term + temp.trim() + " " + symbol + " ";
            }
            term = term.substring(0, term.lastIndexOf(symbol)).trim();
        }
        return term;
    }

}
