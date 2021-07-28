package com.bayee.petition.service;

import org.apache.commons.cli.ParseException;

import java.util.Map;

/**
 * @author antuo
 * @since 2021/7/6 10:01
 */

public interface BoatShipService {

    public Map<String,Object> query(String points,String startDate,String endDate,String attributes) throws ParseException;

    public Map<String,Object> queryAisPoint(String points,String startDate,String endDate,String attributes) throws ParseException;

    public Map<String,Object> queryHistoryAisPoint(String points,String startDate,String endDate,String attributes) throws ParseException;

}
