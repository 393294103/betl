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
import com.betl.mysql.mr.mapper.Mysql2HdfsMapper;
import com.betl.mysql.mr.model.MysqlModelImplCode;
import com.betl.mysql.mr.reducer.Mysql2HdfsReducer;

public class Mysql2Hdfs {
	private static Logger logger = LoggerFactory.getLogger(Mysql2Hdfs.class);

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		BetlConfiguration bconf = new BetlConfiguration();
		Configuration conf = bconf.getConfiguration(args);
		logger.debug("[main-conf]\t{}", conf);
		DBConfiguration.configureDB(conf, conf.get("mysql.jdbc.driver.class"), conf.get("mysql.jdbc.url"), conf.get("mysql.jdbc.username"), conf.get("mysql.jdbc.password"));

		MysqlModelImplCode mysqlModelImplCode = new MysqlModelImplCode();
		String code = mysqlModelImplCode.gengerate(conf);
		
		@SuppressWarnings("rawtypes")
		Class clazz = mysqlModelImplCode.compile(code);

		if (clazz == null) {
			System.exit(1);
		}

		String fieldStr1 = conf.get("mysql.table.columns");
		String[] colAndTypes1 = fieldStr1.split(Constants.SQL_COLUMN_SPLIT_BY);
		String[] fields = new String[colAndTypes1.length];
		for (int i1 = 0; i1 < colAndTypes1.length; i1++) {
			fields[i1] = colAndTypes1[i1].substring(0, colAndTypes1[i1].indexOf(Constants.SQL_Type_SPLIT_BY));
		}

		Job job = Job.getInstance(conf, conf.get("mapreduce.job.name"));
		job.setJarByClass(Mysql2Hdfs.class);

		DBInputFormat.setInput(job, clazz, conf.get("mysql.table.name"), null, conf.get("mysql.table.orderBy"), fields);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(DBInputFormat.class);

		String outputPath = conf.get("hdfs.uri.default") + Constants.PATH_SEPARATOR_DEFAULT + conf.get("hdfs.path.default") + Constants.PATH_SEPARATOR_DEFAULT + conf.get("mysql.jdbc.schema") + Constants.PATH_SEPARATOR_DEFAULT + conf.get("mysql.table.name");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(Mysql2HdfsMapper.class);
		job.setReducerClass(Mysql2HdfsReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
