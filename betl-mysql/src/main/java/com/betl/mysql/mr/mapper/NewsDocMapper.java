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

import com.betl.mysql.mr.model.NewsDoc;

public class NewsDocMapper extends Mapper<LongWritable, NewsDoc, LongWritable, Text> {
	public void map(LongWritable key, NewsDoc value, Context context) throws IOException, InterruptedException {
		context.write(key, new Text(value.toString()));
	}
}
