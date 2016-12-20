/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月20日下午5:33:57
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.conf;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhl
 *
 */
public class ConfigHelper {
	private Logger logger = LoggerFactory.getLogger(getClass());
	private Configuration conf;

	public ConfigHelper(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public String[] mysqlColumns() {
		String colStr = conf.get("mysql.table.columns");
		logger.debug("[mysqlColumns-colStr]\t{}", colStr);
		String[] col = colStr.split(Constants.SQL_COLUMN_SPLIT_BY);
		return col;
	}

	public String hdfsOutputPath() {
		StringBuilder outpath = new StringBuilder();
		outpath.append(conf.get("hdfs.uri.default"));
		outpath.append(Constants.PATH_SEPARATOR_DEFAULT);
		outpath.append(conf.get("hdfs.path.default"));
		outpath.append(Constants.PATH_SEPARATOR_DEFAULT);
		outpath.append(conf.get("mysql.jdbc.schema"));
		outpath.append(Constants.PATH_SEPARATOR_DEFAULT);
		outpath.append(conf.get("mysql.table.name"));
		String res = outpath.toString();
		logger.debug("[mysqlColumns-outpath]\t{}", res);
		return res;
	}

}
