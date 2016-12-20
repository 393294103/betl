/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日上午11:41:34
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.config.option;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;

/**
 * @author zhl
 *
 */
public class XmlConfigruation  implements ICompentConfiguration{
	private String path=null;
	
	public Configuration getConfiguration() throws IOException {
		
		Configuration conf =new Configuration();
			InputStream iStream=XmlConfigruation.class.getResourceAsStream(path);
			conf.addResource(iStream, path);
			return conf;
	}
	
		
		
	public XmlConfigruation(String path){
		this.path=path;
	}
	
	
	
	

}
