/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月11日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr.partitioner;

import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.hbase.mr.helper.PreSplitHBaseHelper;

public class RegionPartitioner extends Partitioner<Text, Text> {
	private static final Logger logger = LoggerFactory.getLogger(RegionPartitioner.class);
	/**
	 *  @param numPartitions，其实是reducer设置的个数
	 */
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		int resPartition=0;
		if (numPartitions == 0) {
			resPartition=0;
			return resPartition;
		} else {
			Map<String,Object>  powerArgsMap=PreSplitHBaseHelper.powerArgs(numPartitions);
			Integer patitionKeyPos=(Integer) powerArgsMap.get(PreSplitHBaseHelper.POWER_NUM)+1;
			String partitionKey=key.toString().substring(0,patitionKeyPos);
			resPartition=PreSplitHBaseHelper.getPartitionId(partitionKey, powerArgsMap);
			logger.info("[getPartition],key={},patitionKeyPos={},partitionKey={},resPartition={}",
					key,patitionKeyPos,partitionKey,resPartition);
			return resPartition;
		}
	}
}
