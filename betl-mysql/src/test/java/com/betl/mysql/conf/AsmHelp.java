/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.conf;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Administrator
 *
 */
public class AsmHelp {


	public static String PreparedStatementMethod(String clazz){
		if (clazz.toUpperCase().equals("STRING")) {
			return "setString";
		} else if (clazz.toUpperCase().equals("INTEGER")) {
			return "setInt";
		}
		return null;
		
	}
	
	
	public static String setMethodName(String methodName) {
		return "set" + methodName.substring(0, 1).toUpperCase() + methodName.substring(1).toLowerCase();
	}

	public static String getMethodName(String methodName) {
		return "get" + methodName.substring(0, 1).toUpperCase() + methodName.substring(1).toLowerCase();
	}

	public static Map<String, String> colNameAndType(String str) {
		Map<String, String> colNameType = new LinkedHashMap<String, String>();
		String[] cols = str.split(",");
		for (String col : cols) {
			String[] nameType = col.split("@");
			colNameType.put(nameType[0], nameType[1]);
		}
		return colNameType;
	}

	public static String packageWarpType(String clazz) {
		if (clazz.toUpperCase().equals("STRING")) {
			return "Ljava/lang/String;";
		} else if (clazz.toUpperCase().equals("INTEGER")) {
			return "Ljava/lang/Integer;";
		}
		return null;
	}

	public static void main(String[] args) {
		//
		System.out.println(setMethodName("url"));

		Map<String, String> nameType = colNameAndType("url@STRING,title@STRING,content@STRING");
		for (String key : nameType.keySet()) {
			System.out.println(key + nameType.get(key));
		}

	}

}
