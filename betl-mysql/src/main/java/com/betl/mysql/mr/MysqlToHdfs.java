/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr;

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

import com.betl.mysql.conf.ConfigHelper;
import com.betl.mysql.mr.mapper.MysqlToHdfsMapper;
import com.betl.mysql.mr.model.v1.ModelRecordImplCode;
import com.betl.mysql.mr.reducer.MysqlToHdfsReducer;

public class MysqlToHdfs implements Tool {
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
		logger.debug("[main-conf]\t{}", conf);
		ConfigHelper cf = new ConfigHelper(conf);
		ModelRecordImplCode modelRecordImplCode = new ModelRecordImplCode(cf);
		String code = modelRecordImplCode.gengerate();
		String modelClassPath = modelRecordImplCode.compile(code);
		@SuppressWarnings("rawtypes")
		Class clazz = modelRecordImplCode.gengerateClass(modelClassPath);
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
		FileOutputFormat.setOutputPath(job, new Path(cf.hdfsStorePath()));
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
