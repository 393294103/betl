/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月5日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.betl.hbase.mr.common.BasicConstants;
import com.betl.hbase.mr.mapper.UserInfoMapper;
import com.betl.hbase.mr.partitioner.RegionPartitioner;
import com.betl.hbase.mr.reducer.UserInfoReducer;

public class UserBaseInfoJob extends Configured implements Tool {
	
	private Configuration conf;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}


	@Override
	public int run(String[] args) throws Exception {
	Job job = Job.getInstance(conf);
		job.setJobName("zhl@"+conf.get(BasicConstants.MR_JOB_NAME));
		job.setPartitionerClass(RegionPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);
	    job.setJarByClass(UserBaseInfoJob.class);
	    job.setMapperClass(UserInfoMapper.class);
	    job.setReducerClass(UserInfoReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(KeyValue.class);
	    job.setOutputFormatClass(HFileOutputFormat2.class);
	    FileInputFormat.addInputPath(job, new Path(conf.get(BasicConstants.HDFS_INPUT_PATH)));
	    FileOutputFormat.setOutputPath(job, new Path(conf.get(BasicConstants.HDFS_OUTPUT_PATH)));
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	


}
