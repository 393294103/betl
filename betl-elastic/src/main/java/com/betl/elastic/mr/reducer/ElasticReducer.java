/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.elastic.mr.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.Gson;

/**
 * @author Administrator
 *
 */
public class ElasticReducer extends Reducer<Text, Text, NullWritable, BytesWritable> {
	private static Gson gson = new Gson();
	private Map<String, String> map = new HashMap<String, String>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text value : values) {
			String[] splits = value.toString().split("\t");
			if (splits.length >= 3) {
				map.put("url", splits[0]);
				map.put("title", splits[1]);
				map.put("content", splits[2]);
			}
		}

		String json = gson.toJson(map);
		// System.out.println(json);
		BytesWritable reduceOut = new BytesWritable(json.getBytes());
		context.write(NullWritable.get(), reduceOut);

	}
}
