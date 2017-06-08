/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日下午12:00:51
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.config.option;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

/**
 * @author zhl
 *
 */
public class PropsConfigruation implements ICompentConfiguration {

	private Properties props = new Properties();
	Configuration conf;
	
	public Configuration getConfiguration() throws IOException {
		for (Object key : props.keySet()) {
			conf.set(key.toString(), props.getProperty(key.toString()));
		}
		return conf;
	}

	
	private String path = null;

	public PropsConfigruation(Configuration conf,String path) throws IOException {
		this.path = path;
		this.conf=conf;
		loads();
	}

	public PropsConfigruation(Configuration conf,Properties props) {
		this.props=props;
		this.conf=conf;
	}
	
	private void loads() throws IOException {
		FileInputStream in = new FileInputStream(path);
		props.load(in);
	}

	
}
