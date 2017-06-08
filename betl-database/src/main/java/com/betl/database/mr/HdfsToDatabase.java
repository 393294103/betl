/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月20日下午6:44:56
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.database.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.database.common.BasicConstants;
import com.betl.database.common.ConfigHelper;
import com.betl.database.mr.mapper.HdfsToDatabaseMapper;
import com.betl.database.mr.reducer.HdfsToDatabaseReducer;

/**
 * @author zhl
 *
 */
public class HdfsToDatabase implements Tool {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private Configuration conf;
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	
	@Override
	public Configuration getConf() {
		return this.conf;
	}

	
	@Override
	public int run(String[] args) throws Exception {
		ConfigHelper cf = new ConfigHelper(conf);
		logger.info("run,conf={}",conf);
		DBConfiguration.configureDB(conf, 
				conf.get(BasicConstants.DATABASE_JDBC_DRIVERCLASS), 
				conf.get(BasicConstants.DATABASE_JDBC_URL), 
				conf.get(BasicConstants.DATABASE_JDBC_USERNAME), 
				conf.get(BasicConstants.DATABASE_JDBC_PASSWORD));
		Job job = Job.getInstance(conf);
		job.setJarByClass(HdfsToDatabase.class);
		job.setMapperClass(HdfsToDatabaseMapper.class);
		job.setReducerClass(HdfsToDatabaseReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(cf.hdfsStorePath()));

		String[] fields = cf.databaseColumns();
		DBOutputFormat.setOutput(job, conf.get(BasicConstants.DATABASE_TABLE_NAME), fields);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}
