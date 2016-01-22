/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015年12月22日下午3:00:01
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.getl.gstorm.core.common;

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
