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

import com.betl.config.option.BetlConfiguration;
import com.betl.mysql.mr.mapper.NewsDocMapper;
import com.betl.mysql.mr.model.NewsDoc;
import com.betl.mysql.mr.reducer.NewsDocReducer;

public class MysqlToHdfs {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		
		BetlConfiguration bconf=new BetlConfiguration();
		Configuration conf = bconf.getConfiguration(args);
		
		
		
		
		DBConfiguration.configureDB(conf, conf.get("mysql.jdbc.driver.class"), conf.get("mysql.jdbc.url"), conf.get("mysql.jdbc.username"), conf.get("mysql.jdbc.password"));

		String fieldStr = conf.get("hive.table.columns");
		String[] colAndTypes = fieldStr.split(Constants.SQL_COLUMN_SPLIT_BY);
		String[] fields = new String[colAndTypes.length];
		for (int i = 0; i < colAndTypes.length; i++) {
			fields[i] = colAndTypes[i].substring(0, colAndTypes[i].indexOf(Constants.SQL_Type_SPLIT_BY));
		}

		Job job = Job.getInstance(conf, conf.get("mapreduce.job.name"));
		job.setJarByClass(MysqlToHdfs.class);

		DBInputFormat.setInput(job, NewsDoc.class, conf.get("mysql.table.name"), null, conf.get("mysql.table.orderBy"), fields);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(DBInputFormat.class);

		String outputPath = conf.get("hdfs.uri.default") + Constants.PATH_SEPARATOR_DEFAULT + conf.get("hdfs.path.default") + Constants.PATH_SEPARATOR_DEFAULT + conf.get("mysql.jdbc.schema") + Constants.PATH_SEPARATOR_DEFAULT + conf.get("mysql.table.name");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(NewsDocMapper.class);
		job.setReducerClass(NewsDocReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
