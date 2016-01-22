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

import com.betl.mysql.mr.mapper.NewsDocMapper;
import com.betl.mysql.mr.model.NewsDoc;
import com.betl.mysql.mr.reducer.NewsDocReducer;

public class MysqlToHdfs {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		Configuration conf = new Configuration();

		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://10.111.32.203:3306/spiderdb?characterEncoding=UTF-8", "spiderdb", "spiderdb");
		String[] fields = { "url", "title", "content" };

		Job job = Job.getInstance(conf, "MysqlToHdfs");
		job.setJarByClass(MysqlToHdfs.class);

		DBInputFormat.setInput(job, NewsDoc.class, "sinanews", null, "id", fields);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(DBInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.111.32.202:8020/mysql/raw/sinanews"));

		job.setMapperClass(NewsDocMapper.class);
		job.setReducerClass(NewsDocReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
