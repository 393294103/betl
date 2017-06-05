/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年4月8日下午3:56:06
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.mr.reducer;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Administrator
 *
 */
public class CombineSmallFileReducer  extends Reducer<Text, BytesWritable, Text, NullWritable> {
	@Override
	public void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
		for (BytesWritable val : values) {
			context.write(new Text(new String(val.getBytes(),0,val.getLength())),NullWritable.get());
	}
	}
}
