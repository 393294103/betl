/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015年12月12日下午5:23:56
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.gtfd.ghbase.mr.mapper;

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
