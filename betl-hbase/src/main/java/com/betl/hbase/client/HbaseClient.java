/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.betl.hbase.client.model.Document;
import com.betl.hbase.client.util.RedMD5Util;

/**
 * @author Administrator
 *
 */
public class HbaseClient {
	
	
	
	  // 声明静态配置
    static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.111.32.203");
    }

	
	 public static void getResultScann(String tableName) throws IOException {
	        Scan scan = new Scan();
	        ResultScanner rs = null;
	        HTable table = new HTable(conf, Bytes.toBytes(tableName));
	        try {
	            rs = table.getScanner(scan);
	            for (Result r : rs) {
	                for (KeyValue kv : r.list()) {
	                    System.out.println("row:" + Bytes.toString(kv.getRow()));
	                    System.out.println("family:"
	                            + Bytes.toString(kv.getFamily()));
	                    System.out.println("qualifier:"
	                            + Bytes.toString(kv.getQualifier()));
	                    System.out
	                            .println("value:" + Bytes.toString(kv.getValue()));
	                    System.out.println("timestamp:" + kv.getTimestamp());
	                    System.out
	                            .println("-------------------------------------------");
	                }
	            }
	        } finally {
	            rs.close();
	        }
	    }
	 
	 
	 public static void main(String[] args) throws IOException {
		 getResultScann("oripage");
	}
}

