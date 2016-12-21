/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月21日下午1:16:23
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.reflect;

import java.lang.reflect.Field;

/**
 * @author zhl
 *
 */
public class Test {
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return name;
	}
	public String name;


	public static void main(String[] args) {
		try {
			Class<Test> c = Test.class;
			Field field = c.getDeclaredField("name");// 获取字段
			Object obj = c.newInstance();// 实例化对象
			field.set(obj, "aaa");// 为字段赋值
			System.out.println(field.get(obj));
			System.out.println(obj);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
