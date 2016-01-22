/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015年12月12日下午5:27:25
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.gtfd.ghbase.mr.reducer;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

/**
 * @author Administrator
 *
 */
public class WriteHbaseReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{
	    @SuppressWarnings("deprecation")
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
	        String rowKey = key.toString();
	        for (Text value : values) {
				String[] splits=value.toString().split("\t");
				 Put putrow = new Put(rowKey.getBytes());
				 putrow.add("url".getBytes(), "value".getBytes(), splits[0].getBytes()); 
				 putrow.add("title".getBytes(), "value".getBytes(), splits[1].getBytes()); 
				 putrow.add("content".getBytes(), "value".getBytes(), splits[2].getBytes()); 
				 context.write(null, putrow);
	        }
	        
	       
	    }
}
