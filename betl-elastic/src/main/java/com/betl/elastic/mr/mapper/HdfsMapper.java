/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.elastic.mr.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Administrator
 *
 */
public  class HdfsMapper extends Mapper<Object, Text, Text, Text>{
	   public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
		   Text outputKey = new Text(key.toString());
		   Text outputValue = value;
		  context.write(outputKey, outputValue);
	  }
	}