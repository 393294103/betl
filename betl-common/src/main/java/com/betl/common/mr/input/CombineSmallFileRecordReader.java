/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016骞�4鏈�8鏃ヤ笅鍗�2:31:00
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.mr.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * @author Administrator
 *
 */
public class CombineSmallFileRecordReader extends RecordReader<LongWritable,BytesWritable> {

	private CombineFileSplit combineFileSplit;
	private LineRecordReader lineRecordReader=new LineRecordReader();
	private Path[] paths;
	private int totalLength;
	private int currentIndex;
	private float currentProgress=0;
	private LongWritable currentKey;
	//private Text currentValue=new Text();
	private BytesWritable currentValue=new BytesWritable();
	
	public CombineSmallFileRecordReader(CombineFileSplit combineFileSplit,TaskAttemptContext context,Integer index) {
		super();
		this.combineFileSplit=combineFileSplit;
		this.currentIndex=index;
	}
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
	this.combineFileSplit=(CombineFileSplit)split;
	FileSplit fileSplit=new FileSplit(combineFileSplit.getPath(currentIndex),combineFileSplit.getOffset(currentIndex),combineFileSplit.getLength(currentIndex),combineFileSplit.getLocations());
	lineRecordReader.initialize(fileSplit, context);
	this.paths=combineFileSplit.getPaths();
	totalLength=paths.length;
	context.getConfiguration().set("map.input.file.name", combineFileSplit.getPath(currentIndex).getName());
	}

	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(currentIndex>=0&&currentIndex<totalLength){
		return lineRecordReader.nextKeyValue();	
		}else{
		return false;
		}
	}

	
	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
	currentKey=lineRecordReader.getCurrentKey();
	return currentKey;
	}

	
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
	/*	String row =new String(lineRecordReader.getCurrentValue().getBytes(),"GBK");
		currentValue.set(row);
		System.out.println("=============="+currentValue);
		return lineRecordReader.getCurrentValue();*/
		byte[] content=lineRecordReader.getCurrentValue().getBytes();
		currentValue.set(content,0,content.length);
		return currentValue;
	}
	

	
	@Override
	public float getProgress() throws IOException, InterruptedException {
		if(currentIndex>=0 && currentIndex<totalLength){
			currentProgress=(float)currentIndex/totalLength;
			return currentProgress;
		}
		return currentProgress;
	}


	@Override
	public void close() throws IOException {
		lineRecordReader.close();
	}

}
