/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日上午10:30:36
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.config.option;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

/**
 * @author zhl
 *
 */
public class PropsConfigruation implements ICompentConfiguration {

	private Properties props = new Properties();

	public Configuration getConfiguration() throws IOException {
		
		Configuration conf =new Configuration();
		for (Object key : props.keySet()) {
			conf.set(key.toString(), props.getProperty(key.toString()));
		}
		return conf;
	}

	
	private String path = null;

	public PropsConfigruation(String path) throws IOException {
		this.path = path;
		loads();
	}

	public PropsConfigruation(Properties props) {
		this.props=props;
	}
	
	private void loads() throws IOException {
		InputStream in = PropsConfigruation.class.getResourceAsStream(path);
		props.load(in);
	}

	
}
