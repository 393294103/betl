/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月20日下午6:44:56
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr;

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

import com.betl.config.option.BetlConfiguration;
import com.betl.mysql.conf.ConfigHelper;
import com.betl.mysql.mr.mapper.HdfsToMysqlMapper;
import com.betl.mysql.mr.reducer.HdfsToMysqlReducer;

/**
 * @author zhl
 *
 */
public class HdfsToMysql implements Tool {
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
		BetlConfiguration bconf = new BetlConfiguration();
		Configuration conf = bconf.getConfiguration(args);
		logger.debug("[main-conf]\t{}", conf);

		ConfigHelper cf = new ConfigHelper(conf);
		DBConfiguration.configureDB(conf, conf.get("mysql.jdbc.driver.class"), conf.get("mysql.jdbc.url"), conf.get("mysql.jdbc.username"), conf.get("mysql.jdbc.password"));

		Job job = Job.getInstance(conf, conf.get("mapreduce.job.name"));
		job.setJarByClass(HdfsToMysql.class);
		job.setMapperClass(HdfsToMysqlMapper.class);
		job.setReducerClass(HdfsToMysqlReducer.class);

		FileInputFormat.addInputPath(job, new Path(cf.hdfsStorePath()));

		String[] fields = cf.mysqlColumns();
		DBOutputFormat.setOutput(job, conf.get("mysql.table.name"), fields);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
