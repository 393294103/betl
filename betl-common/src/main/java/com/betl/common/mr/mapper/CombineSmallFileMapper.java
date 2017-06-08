/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年4月8日下午3:42:11
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.mr.mapper;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.betl.common.common.BasicConstants;

public class CombineSmallFileMapper extends
Mapper<LongWritable,BytesWritable,Text,BytesWritable>{
private Text file =new Text();
public void map(LongWritable key, BytesWritable value, Context context)
		throws IOException, InterruptedException {
	String fileName=context.getConfiguration().
			get(BasicConstants.MAP_INPUT_FILE_NAME);
	file.set(fileName);
	context.write(file, value);
}
}
