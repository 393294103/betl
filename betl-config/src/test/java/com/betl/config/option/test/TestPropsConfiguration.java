/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日上午10:47:14
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.config.option.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import com.betl.config.option.BetlConfiguration;

/**
 * @author zhl
 *
 */
public class TestPropsConfiguration {
	
	public static void main(String[] args) throws IOException {
		BetlConfiguration bconf = new BetlConfiguration();
		Configuration conf = bconf.getConfiguration(args);
		Iterator<Entry<String,String>> iter=conf.iterator();
		System.out.println(conf.get("hdfs.store.path"));
		System.out.println(conf.get("betl.job.name"));
		
	}
	
	

}
