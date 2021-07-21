package com.bayee.petition.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import com.bayee.petition.domain.HbCatlogDAO;
import com.bayee.petition.service.DataInfoService;
import com.bayee.petition.service.HbCatlogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
public class DashBoardController {

	@Autowired
	DataInfoService dataInfoService;

	@Autowired
	HbCatlogService hbCatlogService;


	// 概览
	@RequestMapping("/DashBoard/data-info")
	@ResponseBody
	public Map<String,Integer> dataInfo(HttpServletResponse response)
			throws UnsupportedOperationException {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		Map<String, Integer> map = new HashMap<>();

		map.put("dataCount", dataInfoService.getDataCount());
		map.put("categoryCount", dataInfoService.getCategoryCount());
		map.put("tableCount", dataInfoService.getTableCount());
		return map;
	}

	// 当前任务
	@RequestMapping("/DashBoard/data-current-job")
	@ResponseBody
	public List<HbCatlogDAO> currentJob(HttpServletResponse response)
			throws UnsupportedOperationException {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");

		return hbCatlogService.getCurrentJob();
	}

	// 数据采集趋势
	@RequestMapping("/DashBoard/data-collection-trend")
	@ResponseBody
	public List<HbCatlogDAO> dataCollectionTrend(HttpServletResponse response)
			throws UnsupportedOperationException {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");

		return hbCatlogService.getDataCollectionTrend();
	}

	// 数据量Top5
	@RequestMapping("/DashBoard/data-top5")
	@ResponseBody
	public List<HbCatlogDAO> dataTop5(HttpServletResponse response)
			throws UnsupportedOperationException {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");

		return hbCatlogService.getDataTop5();
	}

	// 数据分类
	@RequestMapping("/DashBoard/data-category")
	@ResponseBody
	public List<HbCatlogDAO> dataCategory(HttpServletResponse response)
			throws UnsupportedOperationException {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");

		return hbCatlogService.getDataCategory();
	}

	// 数据来源
	@RequestMapping("/DashBoard/data-sources")
	@ResponseBody
	public List<HbCatlogDAO> dataSources(HttpServletResponse response)
			throws UnsupportedOperationException {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");

		return hbCatlogService.getDataSources();
	}


	@RequestMapping(value = { "/DashBoard/list" })
	@ResponseBody
	public Map<String,Object> getNatureTypePages(HbCatlogDAO hbCatlogDAO,HttpServletResponse response) { // 性质父集

		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		int count = hbCatlogService.count(hbCatlogDAO);
		hbCatlogDAO.setPage((hbCatlogDAO.getPage()-1)*hbCatlogDAO.getPageSize());


		List<HbCatlogDAO> hbCatlogDAOList = hbCatlogService.findAllNature(hbCatlogDAO);

		Map<String,Object> map = new HashMap<String,Object>();
		map.put("count",count);
		map.put("hbCatlogDAOList",hbCatlogDAOList);
		return map;
	}
	
	
	@RequestMapping(value = { "/DashBoard/fAllCategory" })
	@ResponseBody
	public List<HbCatlogDAO> DashBoardfAllCategory(HttpServletResponse response) { // 性质父集

		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");


		List<HbCatlogDAO> categoryList = hbCatlogService.getDataCategory();
		return categoryList;
	}
	
	
	@RequestMapping(value = { "/DashBoard/fAllOriginSystem" })
	@ResponseBody
	public List<String> fAllOriginSystem(HttpServletResponse response) { // 性质父集

		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");


		List<String> originList = hbCatlogService.selectOrigin();
		return originList;
	}
	
	
	@RequestMapping(value = { "/table/getDetails" })
	@ResponseBody
	public Map<String,Object> tableGetDetails(Integer id,HttpServletResponse response) { // 性质父集

		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");


		HbCatlogDAO hbCatlog = hbCatlogService.findById(id);
		Map<String ,Object> map=new HashMap<String, Object>();
		
		map.put("hbCatlog",hbCatlog);
		map.put("fields",hbCatlog.getFields().split(","));
		map.put("fieldsCh",hbCatlog.getFieldsCh().split(","));	
		
		return map;
	}
	
}
