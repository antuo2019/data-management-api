package com.bayee.petition.utils;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/** 
* @author  shentuqiwei 
* @version 2019年8月27日 下午1:22:45 
*/
public class HttpClientUtil {

	private static final String APPKEY = "wwp2kl4droajcol8";
	private static final String SIGN = "w1m5673wcrni4yl0";

	/**
	 * 带参数的get请求
	 * @param url
	 * @param param
	 * @return String
	 */
	public static String doGet(String url, Map<String, String> param) {
		// 创建Httpclient对象
		CloseableHttpClient httpclient = HttpClients.createDefault();
 
		String resultString = "";
		CloseableHttpResponse response = null;
		try {
			// 创建uri
			URIBuilder builder = new URIBuilder(url);
			if (param != null) {
				for (String key : param.keySet()) {
					builder.addParameter(key, param.get(key));
				}
			}
			URI uri = builder.build();
			// 创建http GET请求
			HttpGet httpGet = new HttpGet(uri);
			// 执行请求
			response = httpclient.execute(httpGet);
			// 判断返回状态是否为200
			if (response.getStatusLine().getStatusCode() == 200) {
				resultString = EntityUtils.toString(response.getEntity(), "UTF-8");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (response != null) {
					response.close();
				}
				httpclient.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return resultString;
	}
	
	/**
	 * 不带参数的get请求
	 * @param url
	 * @return String
	 */
	public static String doGet(String url) {
		return doGet(url, null);
	}
 
	/**
	 * 带参数的post请求
	 * @param url
	 * @param param
	 * @return String
	 */
	public static String doPost(String url, Map<String, String> param) {
		// 创建Httpclient对象
		CloseableHttpClient httpClient = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		String resultString = "";
		try {
			// 创建Http Post请求
			HttpPost httpPost = new HttpPost(url);
			// 创建参数列表
			if (param != null) {
				List<NameValuePair> paramList = new ArrayList<>();
				for (String key : param.keySet()) {
					paramList.add(new BasicNameValuePair(key, param.get(key)));
				}
				// 模拟表单
				UrlEncodedFormEntity entity = new UrlEncodedFormEntity(paramList);
				httpPost.setEntity(entity);
			}
			// 执行http请求
			response = httpClient.execute(httpPost);
			resultString = EntityUtils.toString(response.getEntity(), "utf-8");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				response.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return resultString;
	}
 
	/**
	 * 不带参数的post请求
	 * @param url
	 * @return String
	 */
	public static String doPost(String url) {
		return doPost(url, null);
	}
	
	/**
	 * 传送json类型的post请求
	 * @param url
	 * @param json
	 * @return String
	 */
	public static String doPostJson(String url, String json) {
		// 创建Httpclient对象
		CloseableHttpClient httpClient = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		String resultString = "";
		try {
			// 创建Http Post请求
			HttpPost httpPost = new HttpPost(url);
			// 创建请求内容
			StringEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);

			httpPost.setEntity(entity);
			httpPost.setHeader("access_token","66B82C5D-9792-4B61-B146-7600E8CE2391");
			httpPost.setHeader("token","dcxx123456");
			// 执行http请求
			response = httpClient.execute(httpPost);
			resultString = EntityUtils.toString(response.getEntity(), "utf-8");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				response.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return resultString;
	}
	
	public static String doPutJson(String url, String json) {
		// 创建Httpclient对象
		System.out.println("AAABBBCCCC");
		CloseableHttpClient httpClient = HttpClients.createDefault();
		CloseableHttpResponse response = null;
		String resultString = "";
		try {
			// 创建Http Post请求
			HttpPut httpPut = new HttpPut(url);
			// 创建请求内容
			StringEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
			System.out.println("XXXXXX:"+entity.toString());
			httpPut.setEntity(entity);
			// 执行http请求
			response = httpClient.execute(httpPut);
			resultString = EntityUtils.toString(response.getEntity(), "utf-8");
			System.out.println("执行http请求:"+resultString);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				response.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return resultString;
	}
	
	public class RequestResult {
		public int code;
		public String message;
		public boolean success;
	}
	
	public class RequestSign {
		public String taskHandleTime;
	}

	public static String sendPost(String url, List<NameValuePair> formparams) {
		UrlEncodedFormEntity uefEntity;
		CloseableHttpClient httpClient = null;
		String resultString = "";
		try {
			httpClient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost(url);
			uefEntity = new UrlEncodedFormEntity(formparams, "UTF-8");
			httppost.setEntity(uefEntity);
			System.out.println("executing request " + httppost.getURI());
			CloseableHttpResponse response = httpClient.execute(httppost);
			try {
				HttpEntity entity = response.getEntity();
				// 执行http请求
				response = httpClient.execute(httppost);
				resultString = EntityUtils.toString(response.getEntity(), "utf-8");
			} finally {
				response.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally { // 关闭连接,释放资源
			try {
				httpClient.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return resultString;
	}

	public static void main(String[] args) {
		HashMap<String, String> dataMap = new HashMap<>();
		dataMap.put("appkey",APPKEY);
		dataMap.put("sign",SIGN);
		dataMap.put("timestamp","1615358948000");
		dataMap.put("type","1");
		dataMap.put("pubName","三亚海事局");
		String url = "http://dataserver.ehanghai.cn/weather/getNavigationsInfosByPage";
		System.out.println(doPost(url, dataMap));
	}
}
