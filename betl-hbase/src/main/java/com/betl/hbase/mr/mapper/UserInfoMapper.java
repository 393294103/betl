/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月10日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr.mapper;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.hbase.mr.common.BasicConstants;

public class UserInfoMapper extends Mapper<LongWritable, Text, Text, Text> {

	//map端程序计数器
	private static enum MONITOR{
			MAP_MOITOR,
			MONITOR_HDFS_ROW_PARSE_ERROR,
			MONITOR_GID_BLANK_ERROR,
			MONITOR_MAP_EXCEPTION_ERROR
		}
	
	private static final Logger logger = LoggerFactory.getLogger(UserInfoMapper.class);

	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
		
		String valStr=value.toString();
		String colSplit=context.getConfiguration().get(BasicConstants.HDFS_COLUMN_SPLIT,"\t");
		String[] colVal=valStr.split(colSplit);
		
		if(colVal.length>=14){
			//组装rowkey
			String gid=colVal[0];
			StringBuilder rowKey=new StringBuilder();
			rowKey.append(DigestUtils.md5Hex(gid).substring(0,7));
			rowKey.append("_").append(gid);
			
			if(StringUtils.isBlank(rowKey)){
				//进行审计
				context.getCounter(MONITOR.MAP_MOITOR.toString(), MONITOR.MONITOR_GID_BLANK_ERROR.toString()).increment(1);
				logger.error("[map],{},row={}",MONITOR.MONITOR_GID_BLANK_ERROR.toString(),valStr);
			}else{
				Text mapOutKey =new Text(rowKey.toString());
				Text mapOutVal =value;
				context.write(mapOutKey, mapOutVal);
			}
			
		}else{
			context.getCounter(MONITOR.MAP_MOITOR.toString(), MONITOR.MONITOR_HDFS_ROW_PARSE_ERROR.toString()).increment(1);
			logger.error("[map],{},colVal={}",MONITOR.MONITOR_HDFS_ROW_PARSE_ERROR.toString(),colVal);
		}
		
		} catch (Exception e) {
			context.getCounter(MONITOR.MAP_MOITOR.toString(), MONITOR.MONITOR_MAP_EXCEPTION_ERROR.toString()).increment(1);
			logger.error("map-Exception={}",e);
		}
		
	}
	
	
	
	
	
}
