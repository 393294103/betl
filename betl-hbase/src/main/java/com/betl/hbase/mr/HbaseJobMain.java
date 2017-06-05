/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月5日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr;


public class HbaseJobMain {
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir","D:\\worksoft\\hadoop-common-2.2.0-bin-master");
		System.setProperty ("HADOOP_USER_NAME" , "dp" );
		
		/*
		Configuration conf = ConfOption.getGenHFileControllerJobConf(args);
		String job = conf.get(BasicConstants.MR_JOB_NAME);
		
		int exitCode = 0;
		if (BasicConstants.MR_JOB_TYPE.USER_INFO_JOB.getKey().equals(job)) {
			UserBaseInfoJob userBaseInfoJob = new UserBaseInfoJob();
			userBaseInfoJob.setConf(conf);
			exitCode = ToolRunner.run(conf, userBaseInfoJob, args);
		}*/
		//System.exit(exitCode);
	}
}
