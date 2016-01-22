/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015��12��14������12:55:33
 * @Desc:
 * @Copyright (c) 2014, �������¾ۺϿƼ����޹�˾ All Rights Reserved.
 */
package com.gtfd.ghbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;

import com.gtfd.ghbase.client.model.Document;
import com.gtfd.ghbase.client.util.RedMD5Util;

/**
 * @author Administrator
 *
 */
public class HbaseClient {

	static final private String TABLE_NAME = "sinanews";
	static final private String COL_URL="url";
	static final private String COL_URLMD5="urlMd5";
	static final private String COL_TITLE="title";
	static final private String COL_CONTENT="content";
	
	static private org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();

	static {
		
		hbaseConf.set("hbase.master", "10.111.32.202");  
		//hbaseConf.set("hbase.rootdir", "hdfs://c2.redcloud.cn:8020/apps/hbase/data");
		//hbaseConf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		//hbaseConf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
		hbaseConf.set("hbase.zookeeper.quorum", "10.111.32.202");
		hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");

	}
	private static  HbaseClient redHBaseUtil;
	
	private List<Put> list = new LinkedList<Put>();

	public HbaseClient() {
	}
	
	
	public static HbaseClient getInstance(){
		if(redHBaseUtil==null)
			redHBaseUtil=new HbaseClient();
		return redHBaseUtil;
	}

	public boolean add(Document doc) {
		Put put = new Put(doc.getUid().getBytes());//
        put.add(COL_URL.getBytes(), null, doc.getUrl().getBytes());// 本行数据的第�??��  
        put.add(COL_TITLE.getBytes(), null, doc.getTitle().getBytes());// 本行数据的第三列  
        put.add(COL_CONTENT.getBytes(), null, doc.getContent().getBytes());// 本行数据的第三列  
		list.add(put);
		
		/*if (list.size() > 40000) {
			commit();
		}*/
		if (list.size() > 0) {
			//System.out.println("***************入库HBASE*******************import hbase "+list.size()+" records status:"+commit()+" url:"+doc.getUrl());
			return	commit();
		}
		return false;
	}

	public boolean commit() {
		if (!list.isEmpty()) {
			HTable table = null;
			try {
				table = new HTable(hbaseConf, TABLE_NAME);
				table.put(list);
				table.flushCommits();
				//System.out.println("flushCommit");
				list.clear();
				return true;
			} catch (RetriesExhaustedWithDetailsException e) {
				//WriteUtil.writeMsg("hbase.log", e.getMessage());
			} catch (InterruptedIOException e) {
				//WriteUtil.writeMsg("hbase.log", e.getMessage());
			} catch (Exception e) {
				//WriteUtil.writeMsg("hbase.log", e.getMessage());
			} finally {
				if (table != null)
					try {
						table.close();
						System.out.println("table close");
					} catch (IOException e) {
						//WriteUtil.writeMsg("hbase.log", e.getMessage());
					}
			}
			return false;
		}
		return true;
	}


	@SuppressWarnings("deprecation")
	public Document  get(String key) {
		HTable table = null;
		try {
			table = new HTable(hbaseConf, TABLE_NAME);
			org.apache.hadoop.hbase.client.Result rs = table.get(new Get(RedMD5Util.generate(key).getBytes()));
			Document doc =new Document();
			 for (KeyValue kv : rs.raw()) { 
				 if(COL_URL.equals(new String(kv.getFamily()))){
					 doc.setUrl(new String(kv.getValue()));
					 doc.setUid(RedMD5Util.generate(key));
				 }else if(COL_TITLE.equals(new String(kv.getFamily()))){
					 doc.setTitle(new String(kv.getValue()));
				 }
				 else if(COL_CONTENT.equals(new String(kv.getFamily()))){
					 doc.setContent(new String(kv.getValue()));
				 }
		        }  
			 return doc;
		} catch (IOException e) {
			//WriteUtil.writeMsg("hbase.log", e.getMessage());
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (IOException e) {
				//WriteUtil.writeMsg("hbase.log", e.getMessage());
			}
		}
		return null;
	}

	@SuppressWarnings("deprecation")
	public boolean ifExists (String key) {
		HTable table = null;
		try {
			table = new HTable(hbaseConf, TABLE_NAME);
			org.apache.hadoop.hbase.client.Result rs = table.get(new Get(RedMD5Util.generate(key).getBytes()));
			System.out.println("Checking url to Hbase++++++++++++++++"+(rs.size() !=0 ? true : false));
			return rs.size() !=0 ? true : false;
			
		} catch (IOException e) {
			//WriteUtil.writeMsg("hbase.log", e.getMessage());
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (IOException e) {
				//WriteUtil.writeMsg("hbase.log", e.getMessage());
			}
		}
		
		return false;
	}
	
	 public static void scan() { 
		
		 HTable table = null;
			try {
				table = new HTable(hbaseConf, TABLE_NAME);
				 ResultScanner rs = table.getScanner(new Scan()); 
		            for (Result r : rs) { 
		                //System.out.println("获得到rowkey:" + new String(r.getRow())); 
		                for (KeyValue keyValue : r.raw()) { 
		                    System.out.println("列：" + new String(keyValue.getFamily(),"GBK") 
		                            + "====" + new String(keyValue.getValue(),"GBK")); 
		                } 
		            } 
				return ;
			} catch (IOException e) {
				//WriteUtil.writeMsg("hbase.log", e.getMessage());
			} finally {
				try {
					if (table != null)
						table.close();
				} catch (IOException e) {
					//WriteUtil.writeMsg("hbase.log", e.getMessage());
				}
			}
			return ;
		 
	    } 
	
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		
		HbaseClient ih=HbaseClient.getInstance();
	
		
		/*Document doc=ih.get("http://huaxi.media.baidu.com/article/18224361332895847848");
		System.out.println(doc.getUid());
		System.out.println(doc.getUrl());
		System.out.println(doc.getContent());
		System.out.println(doc.getTitle());
		
		System.out.println("--------------------------------------");
		System.out.println(ih.ifExists("http://huaxi.media.baidu.com/article/18224361332895847848"));
		*/
		//ih.add(new Document("http://huaxi.media.baidu.com/article/18224361332895847848","广东天津福建自贸区�?体方案获�??四地自贸区出征新使命","按照以往流程，自由贸易试验区总体方案经由部委提交国务院，由国务院审批通过即可。�?从中央政治局�??��审议有关方案来看，说明中央非常重视自贸区改革，不仅把自贸区改革放在国家层面，而且对四地自贸区在新常�?下寄予厚望�?”孙元欣说�?"));
		ih.scan();
	}
	
	

}
