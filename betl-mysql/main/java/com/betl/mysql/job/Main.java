/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月20日下午6:16:49
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.config.option.BetlConfiguration;
import com.betl.mysql.conf.Constants;
import com.betl.mysql.mr.HdfsToMysql;
import com.betl.mysql.mr.MysqlToHdfs;

/**
 * @author zhl
 *
 */
public class Main {

	private static Logger logger = LoggerFactory.getLogger(MysqlToHdfs.class);

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		BetlConfiguration bconf = new BetlConfiguration();
		Configuration conf = bconf.getConfiguration(args);
		logger.debug("[main-conf]\t{}", conf);
		if (Constants.BETL_JOB_HDFSTOMYSQL.equals(conf.get("betl.job.name"))) {
			HdfsToMysql hdfsToMysql = new HdfsToMysql();
			hdfsToMysql.setConf(conf);
			ToolRunner.run(conf, hdfsToMysql, args);
		} else if (Constants.BETL_JOB_MYSQLTOHDFS.equals(conf.get("betl.job.name"))) {
			MysqlToHdfs msyqlToHdfs = new MysqlToHdfs();
			msyqlToHdfs.setConf(conf);
			ToolRunner.run(conf, msyqlToHdfs, args);
		} else {
			logger.error("the arg betl.job.name is wrong!");
		}

	}

}
