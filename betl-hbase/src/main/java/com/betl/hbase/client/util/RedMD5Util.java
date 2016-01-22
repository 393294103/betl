/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.client.util;

import java.security.MessageDigest;

public class RedMD5Util {

	public static String generate(String inStr) {
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			System.out.println(e.toString());
			e.printStackTrace();
			return "";
		}
		char[] charArray = inStr.toCharArray();
		byte[] byteArray = new byte[charArray.length];

		for (int i = 0; i < charArray.length; i++)
			byteArray[i] = (byte) charArray[i];

		byte[] md5Bytes = md5.digest(byteArray);
		StringBuffer hexValue = new StringBuffer();
		for (int i = 0; i < md5Bytes.length; i++) {
			int val = ((int) md5Bytes[i]) & 0xff;
			if (val < 16)
				hexValue.append("0");
			// 将byte类型数据转换�?6进制数据，并拼接成字符串
			hexValue.append(Integer.toHexString(val));
		}
		return hexValue.toString();

	}

	public static void main(String[] args) {
		RedMD5Util rmd = new RedMD5Util();
		System.out.println(rmd.generate("nihao"));
	}

}