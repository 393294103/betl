/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.mysql.mr.model.IMysqlModel;



public class Mysql2HdfsMapper extends Mapper<LongWritable, IMysqlModel, LongWritable, Text> {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	
	public void map(LongWritable key, IMysqlModel value, Context context) throws IOException, InterruptedException {
		logger.debug("[map-val]\t{}"+value.formatHdfsStr());
		context.write(key, new Text(value.formatHdfsStr()));
	}
}
