/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日下午2:34:34
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.config.option.test;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

import com.betl.config.option.BetlConfiguration;

/**
 * @author zhl
 *
 */
public class TestBetlConfiguration {
	
	
	
	@Test
	public void test() throws IOException{
		String[] args={"-x public.xml","-p log4j.properties","-Dabc=abc"};
			BetlConfiguration bconf=new BetlConfiguration();
			System.out.println(bconf.getConfiguration(args));
			System.out.println(bconf.getConfiguration(args).get("log4j.rootLogger"));
			System.out.println(bconf.getConfiguration(args).get("mysql.jdbc.username"));
			System.out.println(bconf.getConfiguration(args).get("abc"));
		
		
		
	}

}
