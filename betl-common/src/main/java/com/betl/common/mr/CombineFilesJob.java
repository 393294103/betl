/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年4月8日下午3:46:19
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.mr;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.common.common.BasicConstants;
import com.betl.common.mr.input.CombineSmallFileInputFormat;
import com.betl.common.mr.mapper.CombineSmallFileMapper;
import com.betl.common.mr.reducer.CombineSmallFileReducer;
import com.betl.common.util.HdfsUtil;

/**
 * @author Administrator
 *
 */
public class CombineFilesJob extends Configured implements Tool {
	private static Logger logger = LoggerFactory.getLogger(CombineFilesJob.class);
	
	private Configuration conf;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
	
	private int getReduceNum(Configuration conf) throws IOException, URISyntaxException{
		FileSystem fs=FileSystem.get(conf);
		HdfsUtil hdfsUtil= new HdfsUtil(fs);
		
		String input=conf.get(BasicConstants.HDFS_INPUT_PATH);
		logger.debug("getReduceNum,input={}",input);
		List<String> listFile =hdfsUtil.listFiles(input);
		Long totalSize=0L;
		for (String file : listFile) {
			totalSize=totalSize+hdfsUtil.fileSize(file);
		}
		
		int reduceNum=(int) (totalSize/(1024*1024*64));
		if(reduceNum<1){
			reduceNum=1;
		}
		fs.close();
		return reduceNum;
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		conf.setInt("mapred.min.split.size", 1);
		conf.setLong("mapred.max.split.size", 26214400);//25m
		
		@SuppressWarnings("deprecation")
		Job job=new Job(conf,BasicConstants.MR_JOB_TYPE.COMBINE_FILES_JOB.getKey());
		job.setJarByClass(CombineFilesJob.class);
		job.setMapperClass(CombineSmallFileMapper.class);
		job.setReducerClass(CombineSmallFileReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		
		job.setNumReduceTasks(getReduceNum(conf));
		job.setInputFormatClass(CombineSmallFileInputFormat.class);
		

		String input =conf.get(BasicConstants.HDFS_INPUT_PATH);
		String output=conf.get(BasicConstants.HDFS_OUTPUT_PATH);
		logger.debug("run,input={}",input);
		logger.debug("run,output={}",output);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		int exitFlag = job.waitForCompletion(true) ? 0 : 1;
		return exitFlag;
	}
	
	
	
}
