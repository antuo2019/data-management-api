/**
 * 
 */
package com.bayee.petition.controller;

import com.bayee.petition.service.CallInterfaceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;

@Controller
public class InterfaceController {

	@Autowired
	private CallInterfaceService callInterfaceService;

	@RequestMapping(value = "/Interface/getAisMap",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getAisMap(HttpServletResponse response, String minLon, String maxLon, String minLat, String maxLat, String precision){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getAisMap(minLon,maxLon,minLat,maxLat,precision);
	}

	@RequestMapping(value = "/Interface/shipListData" ,produces = "application/json; charset=utf-8")
	@ResponseBody
	public String shipListData(HttpServletResponse response, String minLon, String maxLon, String minLat, String maxLat){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		System.out.println(callInterfaceService.shipListData(minLon,maxLon,minLat,maxLat));
		return callInterfaceService.shipListData(minLon,maxLon,minLat,maxLat);
	}

	@RequestMapping(value = "/Interface/getShipInfo",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getShipInfo(HttpServletResponse response, String mmsi){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getShipInfo(mmsi);
	}

	@RequestMapping(value = "/Interface/searchShip",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String searchShip(HttpServletResponse response, String keyWord){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.searchShip(keyWord);
	}

	@RequestMapping(value = "/Interface/shipTrajectoryByHbase",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String shipTrajectoryByHbase(HttpServletResponse response, String mmsi, String startTime, String endTime){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.shipTrajectoryByHbase(mmsi,startTime,endTime);
	}

	@RequestMapping(value = "/Interface/getMyPorts",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getMyPorts(HttpServletResponse response){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getMyPorts();
	}

	@RequestMapping(value = "/Interface/getTideByPort",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getTideByPort(HttpServletResponse response, String code){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getTideByPort(code);
	}

	@RequestMapping(value = "/Interface/findComplexByYearOrName",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String findComplexByYearOrName(HttpServletResponse response, String year, String name){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.findComplexByYearOrName(year,name);
	}

	@RequestMapping(value = "/Interface/findComplexNow",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String findComplexNow(HttpServletResponse response){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.findComplexNow();
	}

	@RequestMapping(value = "/Interface/findComplexByComplexId",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String findComplexByComplexId(HttpServletResponse response, String id){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.findComplexByComplexId(id);
	}

	@RequestMapping(value = "/Interface/harbourType",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String harbourType(HttpServletResponse response){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.harbourType();
	}

	@RequestMapping(value = "/Interface/harbour",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String harbour(HttpServletResponse response, String harbourType){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.harbour(harbourType);
	}

	@RequestMapping(value = "/Interface/orderLine",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String orderLine(HttpServletResponse response){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.orderLine();
	}

	@RequestMapping(value = "/Interface/find",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String find(HttpServletResponse response, String startCode, String startName, String endCode, String endName, String shipHeight, String depth,
					   String shipton, String laneName){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.find(startCode,startName,endCode,endName,shipHeight,depth,shipton,laneName);
	}

	@RequestMapping(value = "/Interface/getNavigation",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getNavigation(HttpServletResponse response, String code, String lastGetTime){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getNavigation(code,lastGetTime);
	}

	@RequestMapping(value = "/Interface/getNavigationInfoEsId",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getNavigationInfoEsId(HttpServletResponse response, String esId){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getNavigationInfoEsId(esId);
	}

	@RequestMapping(value = "/Interface/findComplexYear",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String findComplexYear(HttpServletResponse response){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.findComplexYear();
	}

	@RequestMapping(value = "/Interface/findComplexByInfo",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String findComplexByInfo(HttpServletResponse response, String tfbh){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.findComplexByInfo(tfbh);
	}

	@RequestMapping(value = "/Interface/findComplexByCitys",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String findComplexByCitys(HttpServletResponse response, String tfbh){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.findComplexByCitys(tfbh);
	}

	@RequestMapping(value = "/Interface/getWindAreaDataList",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getWindAreaDataList(HttpServletResponse response){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getWindAreaDataList();
	}

	@RequestMapping(value = "/Interface/getWindAreaDetailInfo",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getWindAreaDetailInfo(HttpServletResponse response, String url){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getWindAreaDetailInfo(url);
	}

	@RequestMapping(value = "/Interface/getManualForecastByPaged",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getManualForecastByPaged(HttpServletResponse response, String pageNum, String pageSize){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getManualForecastByPaged(pageNum,pageSize);
	}

	@RequestMapping(value = "/Interface/getManualForecastById",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getManualForecastById(HttpServletResponse response, String id){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getManualForecastById(id);
	}

	@RequestMapping(value = "/Interface/cx",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String cx(HttpServletResponse response, String ids){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.cx(ids);
	}

	@RequestMapping(value = "/Interface/getShipInfoListByMMSIFromHbase",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getShipInfoListByMMSIFromHbase(HttpServletResponse response, String mmsis){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getShipInfoListByMMSIFromHbase(mmsis);
	}

	@RequestMapping(value = "/Interface/getAllPubNameByType",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getAllPubNameByType(HttpServletResponse response, String type){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getAllPubNameByType(type);
	}

	@RequestMapping(value = "/Interface/getNavigationsInfosByPage",produces = "application/json; charset=utf-8")
	@ResponseBody
	public String getNavigationsInfosByPage(HttpServletResponse response, String type, String pageName){
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET,POST");
		return callInterfaceService.getNavigationsInfosByPage(type,pageName);
	}


}
