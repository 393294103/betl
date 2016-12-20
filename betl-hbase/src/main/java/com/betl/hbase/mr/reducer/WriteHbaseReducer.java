/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr.reducer;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

/**
 * @author Administrator
 *
 */
public class WriteHbaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
	
	
	
	@SuppressWarnings("deprecation")
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String rowKey = key.toString();
		for (Text value : values) {
			String[] splits = value.toString().split("\t");
			Put putrow = new Put(rowKey.getBytes());
			putrow.add("url".getBytes(), "value".getBytes(), splits[0].getBytes());
			putrow.add("title".getBytes(), "value".getBytes(), splits[1].getBytes());
			putrow.add("content".getBytes(), "value".getBytes(), splits[2].getBytes());
			context.write(null, putrow);
		}

	}
}
