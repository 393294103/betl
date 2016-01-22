/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015��12��12������5:23:56
 * @Desc:
 * @Copyright (c) 2014, �������¾ۺϿƼ����޹�˾ All Rights Reserved.
 */
package com.betl.hbase.mr.mapper;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Administrator
 *
 */
public class ReadHdfsMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String[] splits=value.toString().split("\t");
		context.write(new Text(DigestUtils.md5Hex(splits[0])), value);
	}
}
