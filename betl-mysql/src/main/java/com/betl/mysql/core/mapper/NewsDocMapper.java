package com.betl.mysql.core.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.betl.mysql.core.model.NewsDoc;

public class NewsDocMapper extends Mapper<LongWritable, NewsDoc, LongWritable, Text> {
	public void map(LongWritable key, NewsDoc value, Context context) throws IOException, InterruptedException {
		context.write(key, new Text(value.toString()));
	}
}
