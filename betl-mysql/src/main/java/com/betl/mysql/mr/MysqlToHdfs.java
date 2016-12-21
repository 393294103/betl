/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.config.option.BetlConfiguration;
import com.betl.mysql.conf.ConfigHelper;
import com.betl.mysql.mr.mapper.MysqlToHdfsMapper;
import com.betl.mysql.mr.model.ModelRecordImplCode;
import com.betl.mysql.mr.reducer.MysqlToHdfsReducer;

public class MysqlToHdfs {
	private static Logger logger = LoggerFactory.getLogger(MysqlToHdfs.class);

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		BetlConfiguration bconf = new BetlConfiguration();
		Configuration conf = bconf.getConfiguration(args);
		logger.debug("[main-conf]\t{}", conf);
		ConfigHelper cf = new ConfigHelper(conf);
		ModelRecordImplCode modelRecordImplCode = new ModelRecordImplCode(cf);
		String code = modelRecordImplCode.gengerate();
		String modelClassPath = modelRecordImplCode.compile(code);
		@SuppressWarnings("rawtypes")
		Class clazz=modelRecordImplCode.gengerateClass(modelClassPath);
		if (clazz == null) {
			System.exit(1);
		}

		DBConfiguration.configureDB(conf, conf.get("mysql.jdbc.driver.class"), conf.get("mysql.jdbc.url"), conf.get("mysql.jdbc.username"), conf.get("mysql.jdbc.password"));
		
		Job job = Job.getInstance(conf, conf.get("mapreduce.job.name"));
		job.setJarByClass(MysqlToHdfs.class);
		job.setMapperClass(MysqlToHdfsMapper.class);
		job.setReducerClass(MysqlToHdfsReducer.class);

		String[] fields = cf.mysqlColumns();
		DBInputFormat.setInput(job, clazz, conf.get("mysql.table.name"), null, conf.get("mysql.table.orderBy"), fields);
		job.setInputFormatClass(DBInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(cf.hdfsOutputPath()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
