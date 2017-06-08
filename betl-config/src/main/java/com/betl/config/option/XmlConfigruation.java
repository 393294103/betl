/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日上午11:41:34
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.config.option;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * @author zhl
 *
 */
public class XmlConfigruation  implements ICompentConfiguration{
	private String path;
	
	public Configuration getConfiguration() throws IOException {
		Configuration conf =new Configuration();
		conf.addResource(new File(path).toURI().toURL());;
		return conf;
	}
	
		
		
	public XmlConfigruation(String path){
		this.path=path;
	}
	
	
	
	

}
