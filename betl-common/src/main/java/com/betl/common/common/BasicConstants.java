/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月10日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.common;

import java.util.HashMap;
import java.util.Map;


public class BasicConstants {
	
	public static final String BETL_JOB_NAME = "betl.job.name";
	
	public static final String ORC_INPUT_SCHEMA="orc.input.schema";
	
	public static final String HDFS_INPUT_PATH = "hdfs.input.path";
	public static final String HDFS_OUTPUT_PATH = "hdfs.output.path";
	public static final String HDFS_COLUMN_SPLIT = "hdfs.column.split";
	
	public static final String MAP_INPUT_FILE_NAME = "map.input.file.name";
	
	
	
	public static enum MR_JOB_TYPE {
		ORC2TEXT_JOB("ORC2TEXT_JOB", "ORC2TEXT_JOB"),
		COMBINE_FILES_JOB("COMBINE_FILES_JOB", "COMBINE_FILES_JOB");
		
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
	

}
