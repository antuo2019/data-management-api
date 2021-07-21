/**
 * 
 */
package com.bayee.petition.controller;

import java.lang.reflect.Modifier;
import java.nio.charset.Charset;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.StringUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author shawnkuo
 *
 */
public class BaseController {
	
	public final static String SESSION_USER = "user";
	
	protected final HttpHeaders headers;  
	protected final Gson gson;
	

	public BaseController () {
		gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.TRANSIENT)
				.create();
		headers = new HttpHeaders();
		MediaType mediaType=new MediaType("text","json",Charset.forName("UTF-8"));
		headers.setContentType(mediaType);
    }
	
	public String getRequestURL(HttpServletRequest request) {
		String requestUrl = request.getRequestURL().toString();
		String queryString = request.getQueryString();
		if(!StringUtils.isEmpty(queryString)) {
			requestUrl += "?" + queryString;
		}
		return requestUrl;
	}
	
}
