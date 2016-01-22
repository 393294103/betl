/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015�?10�?27日下�?6:27:33
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.betl.elastic.core.job;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import com.google.gson.Gson;
public class Hdfs2ElasticByJosn {
	 
public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
   public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	   Text outputKey = new Text(key.toString());
	   Text outputValue = value;
	  context.write(outputKey, outputValue);
  }
}

public static class IntSumReducer  extends Reducer<Text,Text,NullWritable,BytesWritable> {
	private static Gson gson = new Gson();
	private Map<String,String> map=new HashMap<String,String>();
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
	  
	  for (Text value : values) {
			String[] splits=value.toString().split("\t");
			if(splits.length>=3){
			map.put("url", splits[0]);
			map.put("title", splits[1]);
			map.put("content", splits[2]);
			}
		}

		  String json = gson.toJson(map);
		  //System.out.println(json);
		  BytesWritable reduceOut = new BytesWritable(json.getBytes());
		  context.write(NullWritable.get(), reduceOut);
	  
  }
}

public static void main(String[] args) throws Exception {
System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
System.setProperty ("HADOOP_USER_NAME" , "hdfs" );


Configuration conf = new Configuration();
/*conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
conf.setBoolean("mapred.map.tasks.speculative.execution", false);*/
conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
conf.set("es.nodes", "10.111.32.203");
conf.set("es.resource", "news/sina");  
conf.set("es.input.json", "yes");

  Job job = Job.getInstance(conf, "sinanews");
  job.setJarByClass(Hdfs2ElasticByJosn.class);
  
  job.setMapperClass(TokenizerMapper.class);
  
// job.setCombinerClass(IntSumReducer.class);
 
  job.setReducerClass(IntSumReducer.class);
  
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(Text.class);
  
  job.setOutputFormatClass(EsOutputFormat.class);
  FileInputFormat.addInputPath(job, new Path("hdfs://10.111.32.202:8020/storm/process/sinanews"));
  //FileOutputFormat.setOutputPath(job, new Path("hdfs://10.111.32.202:8020/zhl/output"));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}

