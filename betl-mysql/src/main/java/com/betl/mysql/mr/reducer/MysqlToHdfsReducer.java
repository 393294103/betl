/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Administrator
 *
 */
public class MysqlToHdfsReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text val : values) {
			//logger.debug("[reduce-val]\t{}", val.toString());
			context.write(val, NullWritable.get());
		}

	}
}
