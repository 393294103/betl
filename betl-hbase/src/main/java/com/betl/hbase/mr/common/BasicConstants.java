/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月10日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr.common;

import java.util.HashMap;
import java.util.Map;

public class BasicConstants {
	
	
	
	public static final String MAPREDUCE_JOB_REDUCES = "mapreduce.job.reduces";

	public static final String MR_JOB_NAME = "mr.job.name";
	
	public static final String HDFS_INPUT_PATH = "hdfs.input.path";
	public static final String HDFS_OUTPUT_PATH = "hdfs.output.path";
	public static final String HDFS_COLUMN_SPLIT = "hdfs.column.split";
	public static final String HDFS_INPUT_DATE = "hdfs.input.date";
	
	
	
	public static final String HBASE_LOAD_TAB = "hbase.load.tab";
	public static final String HBASE_COLUMN_FAMILY = "hbase.column.family";
	public static final String HBASE_TAB_NAME = "hbase.tab.name";
	public static final String HBASE_REGION_NUM = "hbase.region.num";
	
	
/*	public static final String MYSQL_JDBC_HOST = "mysql.jdbc.host";
	public static final String MYSQL_JDBC_SCHEMA = "mysql.jdbc.schema";
	public static final String MYSQL_JDBC_USERNAME = "mysql.jdbc.username";
	public static final String MYSQL_JDBC_PASSWORD = "mysql.jdbc.password";*/
	
	
	
	public  static final String MR_ROW_SPLITBY_T="\t";
	
	

	public static enum MR_JOB_TYPE {
		TAG_WEIGHT_JOB("TAG_WEIGHT_JOB", "TAG_WEIGHT_JOB");
		
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
			Map<String, String> outFileTypeMap = new HashMap<String, String>();
			for (MR_JOB_TYPE o : MR_JOB_TYPE.values()) {
				outFileTypeMap.put(o.getKey(), o.getVal());
			}
			return outFileTypeMap;
		}
	};

	public static enum USER_BASE_INFO_FIELD {
		GID("gid"),GENDER("gender"),AGE("age"),AREA("area"),HOUSE("house"),
		CAR("car"),HAS_CHILD("has_child"),PHONE_BRAND("phone_brand"),MON_DV("mon_dv"),
		NEEDS("needs"), PREFS("prefs"),APPS("apps"),LAST_ONLINE("last_online");
		private String val;

		private USER_BASE_INFO_FIELD(String val) {
			this.val = val;
		}

		public String getVal() {
			return this.val;
		}
	}

}
