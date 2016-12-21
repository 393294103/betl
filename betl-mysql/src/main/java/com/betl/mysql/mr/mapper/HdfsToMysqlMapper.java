/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月21日上午10:32:40
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhl
 *
 */
public class HdfsToMysqlMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	private Logger logger = LoggerFactory.getLogger(getClass());

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		logger.debug("[map-val]\t{}", value);
		context.write(key, value);
	}

}
