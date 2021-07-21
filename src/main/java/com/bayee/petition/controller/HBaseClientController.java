package com.bayee.petition.controller;

import com.bayee.petition.service.HBaseClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;
import java.util.Map;

@Controller
public class HBaseClientController {


    @Autowired
    private HBaseClientService hBaseClientService;

    @RequestMapping("/HBaseClient/countData")
    @ResponseBody
    public Map<String, Object> getCountData(HttpServletResponse response,String tableName) {
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");
        return hBaseClientService.getCount(tableName);
    }

    @RequestMapping("/HBaseClient/allData")
    @ResponseBody
    public Map<String, Object> getAllData(HttpServletResponse response, String tableName,@RequestParam(required = false,defaultValue = "*") String fields) {
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");
        return hBaseClientService.getAllData(tableName,fields);
    }

    @RequestMapping("/HBaseClient/pageData")
    @ResponseBody
    public Map<String, Object> getPageData(HttpServletResponse response, String tableName,@RequestParam(required = false,defaultValue = "*") String fields,
                                           @RequestParam(required = false,defaultValue = "1") int pageIndex,
                                           @RequestParam(required = false,defaultValue = "50") int pageSize) {
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");
        return hBaseClientService.getPageData(tableName,fields,pageIndex,pageSize);
    }

    @RequestMapping("/HBaseClient/allSortData")
    @ResponseBody
    public Map<String, Object> getAllSortData(HttpServletResponse response, String tableName,@RequestParam(required = false,defaultValue = "*") String fields,String sort) {
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");
        return hBaseClientService.getAllSortData(tableName,fields,sort);
    }

    @RequestMapping("/HBaseClient/pageSortData")
    @ResponseBody
    public Map<String, Object> getPageSortData(HttpServletResponse response, String tableName,@RequestParam(required = false,defaultValue = "*") String fields,
                                           @RequestParam(required = false,defaultValue = "1") int pageIndex,
                                           @RequestParam(required = false,defaultValue = "50") int pageSize,String sort) {
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");
        return hBaseClientService.getPageSortData(tableName,fields,sort,pageIndex,pageSize);
    }

    @RequestMapping("/HBaseClient/conditionAllData")
    @ResponseBody
    public Map<String, Object> getConditionAllData(HttpServletResponse response,String tableName,@RequestParam(required = false,defaultValue = "*") String fields,
                                                @RequestParam(required = false,defaultValue = "and") String symbol,String condition) {

        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");
        return hBaseClientService.getAllConditionData(tableName,fields,symbol,condition);
    }

    @RequestMapping("/HBaseClient/conditionPageData")
    @ResponseBody
    public Map<String, Object> getConditionPageData(HttpServletResponse response,String tableName,@RequestParam(required = false,defaultValue = "*") String fields,
                                                    @RequestParam(required = false,defaultValue = "and") String symbol,String condition,
                                                    @RequestParam(required = false,defaultValue = "1") int pageIndex,
                                                    @RequestParam(required = false,defaultValue = "50") int pageSize) {

        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");
        return hBaseClientService.getPageConditionData(tableName,fields,symbol,condition,pageIndex,pageSize);
    }

    @RequestMapping("/HBaseClient/conditionPageSortData")
    @ResponseBody
    public Map<String, Object> getConditionPageSortData(HttpServletResponse response,String tableName,@RequestParam(required = false,defaultValue = "*") String fields,
                                                    @RequestParam(required = false,defaultValue = "and") String symbol,String condition,
                                                    @RequestParam(required = false,defaultValue = "1") int pageIndex,
                                                    @RequestParam(required = false,defaultValue = "50") int pageSize,
                                                    String sort) {

        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");
        return hBaseClientService.getPageConditionSortData(tableName,fields,symbol,condition,sort,pageIndex,pageSize);
    }



    @RequestMapping("/HBaseClient/conditionAllSortData")
    @ResponseBody
    public Map<String, Object> getConditionAllSortData(HttpServletResponse response,String tableName,@RequestParam(required = false,defaultValue = "*") String fields,
                                                @RequestParam(required = false,defaultValue = "and") String symbol,String condition,String sort) {

        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");
        return hBaseClientService.getAllConditionSortData(tableName,fields,symbol,condition,sort);
    }

}
