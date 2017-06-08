/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.database.common;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Administrator
 *
 */
public class BasicConstants {

	
	public final static String BETL_JOB_NAME = "betl.job.name";
	
	public final static String BETL_DATABASE_NAME = "betl.database.name";

	public final static String SQL_COLUMN_SPLITBY = ",";


	public final static String DATABASE_JDBC_DRIVERCLASS = "database.jdbc.driverClass";
	public final static String DATABASE_JDBC_URL = "database.jdbc.url";
	public final static String DATABASE_JDBC_SCHEMA = "database.jdbc.schema";
	public final static String DATABASE_JDBC_USERNAME = "database.jdbc.username";
	public final static String DATABASE_JDBC_PASSWORD = "database.jdbc.password";
	
	
	
	public final static String DATABASE_TABLE_NAME = "database.table.name";
	public final static String DATABASE_TABLE_COLUMNS = "database.table.columns";
	public final static String DATABASE_TABLE_ORDERBY = "database.table.orderBy";
	
	public final static String HDFS_COLUMNS_SPLIT = "hdfs.columns.split";
	public final static String HDFS_STORE_PATH = "hdfs.store.path";
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static enum MR_JOB_TYPE {
		DATABASE2HDFS_JOB("DATABASE2HDFS_JOB", "DATABASE2HDFS_JOB"),
		HDFS2DATABASE_JOB("HDFS2DATABASE_JOB", "HDFS2DATABASE_JOB");
		
		private String key;
		private String val;

		private MR_JOB_TYPE(String key, String val) {
			this.val = val;
			this.key = key;
		}

		public String getKey() {
			return this.key;
		}

		public String getVal() {
			return this.val;
		}

		public static Map<String, String> enum2Map() {
			Map<String, String> mrJobTypeMap = new HashMap<String, String>();
			for (MR_JOB_TYPE o : MR_JOB_TYPE.values()) {
				mrJobTypeMap.put(o.getKey(), o.getVal());
			}
			return mrJobTypeMap;
		}
	};
}
