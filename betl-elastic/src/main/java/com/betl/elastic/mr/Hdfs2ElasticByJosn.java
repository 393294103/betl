/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.elastic.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import com.betl.elastic.mr.mapper.HdfsMapper;
import com.betl.elastic.mr.reducer.ElasticReducer;

public class Hdfs2ElasticByJosn {

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		Configuration conf = new Configuration();
		/*
		 * conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",
		 * true); conf.setBoolean("mapred.map.tasks.speculative.execution",
		 * false);
		 */
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		conf.set("es.nodes", "10.111.32.203");
		conf.set("es.resource", "news/sina");
		conf.set("es.input.json", "yes");

		Job job = Job.getInstance(conf, "sinanews");
		job.setJarByClass(Hdfs2ElasticByJosn.class);

		job.setMapperClass(HdfsMapper.class);

		// job.setCombinerClass(IntSumReducer.class);

		job.setReducerClass(ElasticReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(EsOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://10.111.32.202:8020/storm/process/sinanews"));
		// FileOutputFormat.setOutputPath(job, new
		// Path("hdfs://10.111.32.202:8020/zhl/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
