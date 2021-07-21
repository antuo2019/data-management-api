/**
 * 
 */
package com.bayee.petition.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.util.StringUtils;

/**
 * @author seanguo
 *
 */
public class DateUtils {

	private final static DateFormat DATE_TIME_FORMAT1 = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss");
	
	public final static SimpleDateFormat DATE_TIME_FORMAT2 = new SimpleDateFormat ( "yyyy-MM-dd HH:mm:ss" ) ;
	
	public final static SimpleDateFormat DATE_TIME_FORMAT3 = new SimpleDateFormat ( "yyyy-MM-dd HH:mm:ss" ) ;
	
	public static long getUnixTime(String dateTime) {
		long epoch =0l;
		try {
			epoch = DATE_TIME_FORMAT1.parse(dateTime).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return epoch;
	}
	
	public static Date toDate(String dateStr) {
		try {
			return DATE_TIME_FORMAT2.parse(dateStr);
		} catch (ParseException e) {
			return null;
		}
	}
	
	public static String dateToString(Date date) {
		SimpleDateFormat  sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sf.format(date);
	}

	public static String dateToDayString(Date date) {
		SimpleDateFormat  sf = new SimpleDateFormat("dd");
		return sf.format(date);
	}
	
	public static Date parseDate(String dateStr, String format) {
		if(StringUtils.isEmpty(dateStr)) return null;
		try {
			SimpleDateFormat DATE_FORMAT = new SimpleDateFormat (format) ;
			return DATE_FORMAT.parse(dateStr);
		} catch (ParseException e) {
			return null;
		}
	}
	
	public static String formatDate(Date date, String format) {
		if(date == null) return null;
		DateFormat df = new SimpleDateFormat(format);
		return df.format(date);
	}
}
