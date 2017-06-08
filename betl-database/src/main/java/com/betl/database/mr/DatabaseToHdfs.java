/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.database.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.database.common.BasicConstants;
import com.betl.database.common.ConfigHelper;
import com.betl.database.mr.mapper.DatabaseToHdfsMapper;
import com.betl.database.mr.model.v1.ModelRecordImplCode;
import com.betl.database.mr.reducer.DatabaseToHdfsReducer;

public class DatabaseToHdfs implements Tool {
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
	@SuppressWarnings("unchecked")
	public int run(String[] args) throws Exception {
		logger.debug("run,conf={}", conf);
		ConfigHelper cf = new ConfigHelper(conf);
		ModelRecordImplCode modelRecordImplCode = new ModelRecordImplCode(cf);
		String code = modelRecordImplCode.gengerate();
		String modelClassPath = modelRecordImplCode.compile(code);
		@SuppressWarnings("rawtypes")
		Class clazz = modelRecordImplCode.gengerateClass(modelClassPath);
		if (clazz == null) {
			System.exit(1);
		}
		DBConfiguration.configureDB(conf, 
				conf.get(BasicConstants.DATABASE_JDBC_DRIVERCLASS),
				conf.get(BasicConstants.DATABASE_JDBC_URL), 
				conf.get(BasicConstants.DATABASE_JDBC_USERNAME), 
				conf.get(BasicConstants.DATABASE_JDBC_PASSWORD));
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(DatabaseToHdfs.class);
		job.setMapperClass(DatabaseToHdfsMapper.class);
		job.setReducerClass(DatabaseToHdfsReducer.class);

		String[] fields = cf.databaseColumns();
		DBInputFormat.setInput(job, clazz, 
				conf.get(BasicConstants.DATABASE_TABLE_NAME),null, 
				conf.get(BasicConstants.DATABASE_TABLE_ORDERBY), fields);
		
		job.setInputFormatClass(DBInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(cf.hdfsStorePath()));
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
