/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年6月5日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.mr.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.betl.common.mr.common.BasicConstants;
import com.betl.common.mr.mapper.Orc2TextMapper;

public class Orc2TextJob extends Configured implements Tool {

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
		job.setNumReduceTasks(0);
		job.setJobName("zhl@"+conf.get(BasicConstants.MR_JOB_TYPE.ORC2TEXT_JOB.getKey()));
		job.setInputFormatClass(OrcNewInputFormat.class);
		job.setJarByClass(Orc2TextJob.class);
		job.setMapperClass(Orc2TextMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		String input=conf.get(BasicConstants.HDFS_INPUT_PATH);
		String output=conf.get(BasicConstants.HDFS_OUTPUT_PATH);
		
		FileInputFormat.addInputPath(job,
				new Path(input));
		FileOutputFormat.setOutputPath(job,
				new Path(output));
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
