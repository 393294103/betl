/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年4月8日下午3:30:06
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.mr.input;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * @author Administrator
 *
 */
public class CombineSmallFileInputFormat 
extends CombineFileInputFormat<LongWritable, BytesWritable> {

	 @Override
	 public RecordReader<LongWritable, BytesWritable> 
	 createRecordReader(InputSplit split, TaskAttemptContext context) 
			 throws IOException{
		 CombineFileSplit combineFileSplit=(CombineFileSplit)split;
		 CombineFileRecordReader<LongWritable,BytesWritable> recordReader
		 =new CombineFileRecordReader<LongWritable, BytesWritable>
		 (combineFileSplit, context, CombineSmallFileRecordReader.class);
		 try {
			recordReader.initialize(combineFileSplit, context);
		} catch (InterruptedException e) {
			new RuntimeException("Error to initalize CombineSmallFileRecordReader.");
		}
		 return recordReader;
	 }

	

}
