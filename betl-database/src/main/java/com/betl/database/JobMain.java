/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年6月8日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.database;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.config.option.BetlConfiguration;
import com.betl.database.common.BasicConstants;
import com.betl.database.mr.DatabaseToHdfs;
import com.betl.database.mr.HdfsToDatabase;

public class JobMain {
	private static Logger logger = LoggerFactory.getLogger(JobMain.class);

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir",
				"D:\\worksoft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		BetlConfiguration bconf = new BetlConfiguration();
		Configuration conf = bconf.getConfiguration(args);
		String betlJobName = conf.get(BasicConstants.BETL_JOB_NAME);
		logger.debug("main,conf={}", conf);
		if (BasicConstants.MR_JOB_TYPE.DATABASE2HDFS_JOB.getKey()
				.equals(betlJobName)) {
			
			DatabaseToHdfs databaseToHdfs = new DatabaseToHdfs();
			databaseToHdfs.setConf(conf);
			
			ToolRunner.run(conf, databaseToHdfs, args);

		} else if (BasicConstants.MR_JOB_TYPE.HDFS2DATABASE_JOB.getKey()
				.equals(betlJobName)) {
			
			HdfsToDatabase hdfsToDatabase = new HdfsToDatabase();
			hdfsToDatabase.setConf(conf);
			
			ToolRunner.run(conf, hdfsToDatabase, args);
		
		} else {
			logger.error("the arg betl.job.name is wrong!");
		}
	}
}
