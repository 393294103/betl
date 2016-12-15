/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日上午10:17:39
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.config.option;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * @author zhl
 *
 */
public interface ICompentConfiguration {

	public Configuration getConfiguration()  throws IOException ;
		
}
