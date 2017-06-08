/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月20日下午5:33:57
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.database.common;

import java.io.File;

import org.apache.commons.lang.StringUtils;
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

	public String[] databaseColumns() {
		String colStr = conf.get(BasicConstants.DATABASE_TABLE_COLUMNS);
		logger.debug("mysqlColumns,colStr={}", colStr);
		String[] col = colStr.split(BasicConstants.SQL_COLUMN_SPLITBY);
		return col;
	}

	public String hdfsStorePath() {
		String res = StringUtils.EMPTY;
		StringBuilder outpath = new StringBuilder();
		if (StringUtils.isBlank(conf.get(BasicConstants.HDFS_STORE_PATH))) {
			outpath.append(conf.get(BasicConstants.BETL_DATABASE_NAME));
			outpath.append(File.separator);
			outpath.append(conf.get(BasicConstants.DATABASE_JDBC_SCHEMA));
			outpath.append(File.separator);
			outpath.append(conf.get(BasicConstants.DATABASE_TABLE_NAME));
		} else {
			outpath.append(conf.get(BasicConstants.HDFS_STORE_PATH));
		}
		res = outpath.toString();
		logger.debug("hdfsStorePath,output={}", res);
		return res;
	}

}
