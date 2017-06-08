/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年6月5日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.common.common.BasicConstants;
import com.betl.common.mr.CombineFilesJob;
import com.betl.common.mr.Orc2TextJob;
import com.betl.config.option.BetlConfiguration;

public class JobMain {
	private static Logger logger = LoggerFactory.getLogger(JobMain.class);
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\worksoft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		BetlConfiguration bconf = new BetlConfiguration();
		Configuration conf = bconf.getConfiguration(args);
		String betlJobName=conf.get(BasicConstants.BETL_JOB_NAME);
		logger.debug("[main-conf]\t{}", conf);
		if (BasicConstants.MR_JOB_TYPE.COMBINE_FILES_JOB.getKey()
				.equals(betlJobName)) {
			CombineFilesJob combineFilesJob=new CombineFilesJob();
			ToolRunner.run(conf, combineFilesJob, args);
		} else if (BasicConstants.MR_JOB_TYPE.ORC2TEXT_JOB.getKey()
				.equals(betlJobName)) {
			Orc2TextJob orc2TextJob =new Orc2TextJob();
			orc2TextJob.setConf(conf);
			ToolRunner.run(conf, orc2TextJob, args);
		} else {
			logger.error("the arg betl.job.name is wrong!");
		}
		

	}

}
