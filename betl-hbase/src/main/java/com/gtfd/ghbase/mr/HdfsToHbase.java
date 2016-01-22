/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015年12月12日下午5:27:58
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.gtfd.ghbase.mr;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.gtfd.ghbase.mr.mapper.ReadHdfsMapper;
import com.gtfd.ghbase.mr.reducer.WriteHbaseReducer;

/**
 * @author Administrator
 *
 */
public class HdfsToHbase {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "10.111.32.202");  
        conf.set("hbase.zookeeper.quorum", "10.111.32.202");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        
        Job job = new Job(conf,"Hbase");
        job.setJarByClass(HdfsToHbase.class);
        
       
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://10.111.32.202:8020/mysql/raw/sinanews"));
        
        job.setMapperClass(ReadHdfsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        TableMapReduceUtil.initTableReducerJob("sinanews", WriteHbaseReducer.class, job);
        
       job.waitForCompletion(true);
	}

}
