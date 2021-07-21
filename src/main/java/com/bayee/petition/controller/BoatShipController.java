package com.bayee.petition.controller;

import com.bayee.petition.service.BoatShipService;
import org.apache.commons.cli.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

/**
 * @author antuo
 * @since 2021/7/6 10:11
 */
@Controller
@RequestMapping("/boatShip")
public class BoatShipController {

    @Autowired
    private BoatShipService boatShipService;

    @RequestMapping("/query")
    @ResponseBody
    public Map<String, Object> query(String ecql) throws ParseException {
        return boatShipService.query(ecql);
    }

}
