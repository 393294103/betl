/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日上午10:47:14
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.config.option.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.betl.config.option.PropsConfigruation;

/**
 * @author zhl
 *
 */
public class TestPropsConfiguration {
	
	@Test
	public void test1() throws IOException{
		String path="";
		PropsConfigruation pc=new PropsConfigruation("/log4j.properties");
		Configuration conf=pc.getConfiguration();
		System.out.println(conf.get("log4j.rootLogger"));
		
		
		
		
	}
	
	

}
