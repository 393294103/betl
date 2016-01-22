package com.betl.mysql.core.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.betl.mysql.core.model.NewsDoc;

/**
 * @author Administrator
 *
 */
public class NewsDocReducer extends Reducer<LongWritable, Text, NewsDoc, NullWritable> {
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		NewsDoc newsDoc = new NewsDoc();

		for (Text val : values) {
			String[] splits = val.toString().split("\t");
			newsDoc.setUrl(splits[0]);
			newsDoc.setTitle(splits[1]);
			newsDoc.setContent(splits[2]);
		}
		context.write(newsDoc, NullWritable.get());
	}
}
