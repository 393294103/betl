/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.getl.gstorm.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Administrator
 *
 */
public class CommonUtil {
	 public static  String replaceBlank(String str) {
			String dest = "";
			if (str != null) {
				Pattern p = Pattern.compile("\\s*|\t|\r|\n");
				Matcher m = p.matcher(str);
				dest = m.replaceAll("");
			}
			return dest;
		}
}
