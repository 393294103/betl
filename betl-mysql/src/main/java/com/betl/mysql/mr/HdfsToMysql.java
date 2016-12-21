/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月20日下午6:44:56
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.config.option.BetlConfiguration;
import com.betl.mysql.conf.ConfigHelper;
import com.betl.mysql.mr.mapper.HdfsToMysqlMapper;
import com.betl.mysql.mr.model.MysqlModelImplCode;
import com.betl.mysql.mr.reducer.HdfsToMysqlReducer;

/**
 * @author zhl
 *
 */
public class HdfsToMysql {
	private static Logger logger = LoggerFactory.getLogger(HdfsToMysql.class);
	
	
	
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		BetlConfiguration bconf = new BetlConfiguration();
		Configuration conf = bconf.getConfiguration(args);
		logger.debug("[main-conf]\t{}", conf);
		ConfigHelper cf = new ConfigHelper(conf);
		DBConfiguration.configureDB(conf, conf.get("mysql.jdbc.driver.class"), conf.get("mysql.jdbc.url"), conf.get("mysql.jdbc.username"), conf.get("mysql.jdbc.password"));
		
		
		
		MysqlModelImplCode mysqlModelImplCode = new MysqlModelImplCode(cf);
		String code = mysqlModelImplCode.gengerate();
		System.out.println(code);
		
		
		
		Job job = Job.getInstance(conf, conf.get("mapreduce.job.name"));
		job.setJarByClass(HdfsToMysql.class);
		job.setMapperClass(HdfsToMysqlMapper.class);
		job.setReducerClass(HdfsToMysqlReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(cf.hdfsOutputPath()));		
		
		String[] fields=cf.mysqlColumns();
		DBOutputFormat.setOutput(job, conf.get("mysql.table.name"), fields);
		
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
