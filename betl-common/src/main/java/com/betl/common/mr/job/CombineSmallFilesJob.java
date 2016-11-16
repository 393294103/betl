/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年4月8日下午3:46:19
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.mr.job;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.betl.common.mr.input.CombineSmallFileInputFormat;
import com.betl.common.mr.mapper.CombineSmallFileMapper;
import com.betl.common.mr.reducer.CombineSmallFileReducer;
import com.betl.common.util.HdfsUtil;

/**
 * @author Administrator
 *
 */
public class CombineSmallFilesJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		
		
		
		
		Configuration conf=new Configuration();
		
		
		conf.setInt("mapred.min.split.size", 1);
		conf.setLong("mapred.max.split.size", 26214400);//25m
		//conf.setInt("mapred.reduce.tasks", 5);
		//conf.setInt("mapred.reduce.tasks", 1);
		
		FileSystem fs=FileSystem.get(new URI("hdfs://10.111.32.202:8020") ,conf);
		
		
		
		HdfsUtil hdfsUtil= new HdfsUtil(fs);
	
		List<String> listFile =hdfsUtil.listFiles("hdfs://10.111.32.202:8020/storm/raw/test");
		Long totalSize=0L;
		
		for (String file : listFile) {
			totalSize=totalSize+hdfsUtil.fileSize(file);
		}
		
		
		
		
		
		
		
		
		fs.delete(new Path("/storm/raw/test/combine_files"), true);
		
		
		
		
		
		
		
		Job job=new Job(conf,"combine small files");
		job.setJarByClass(CombineSmallFilesJob.class);
		job.setMapperClass(CombineSmallFileMapper.class);
		job.setReducerClass(CombineSmallFileReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		int reduceNum=(int) (totalSize/(1024*1024*64));
		if(reduceNum<1){
			reduceNum=1;
		}
		
		System.out.println(reduceNum);
		
		job.setNumReduceTasks(reduceNum);
		
		/*job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);*/

		job.setInputFormatClass(CombineSmallFileInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(FileOutputFormat.class);

		
		FileInputFormat.addInputPath(job, new Path("hdfs://10.111.32.202:8020/storm/raw/test"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.111.32.202:8020/storm/test/combine_files"));

		int exitFlag = job.waitForCompletion(true) ? 0 : 1;
		System.exit(exitFlag);
		
	}

}
