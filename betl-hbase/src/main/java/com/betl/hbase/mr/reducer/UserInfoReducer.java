/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月16日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr.reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.hbase.mr.common.BasicConstants;

public class UserInfoReducer  extends Reducer<Text, Text, ImmutableBytesWritable, KeyValue> {
	private static final Logger logger = LoggerFactory.getLogger(UserInfoReducer.class);
	private static String cf=StringUtils.EMPTY;
	//map端程序计数器
	private static enum MONITOR{
		REDUCE_MOITOR,
		MONITOR_REDUCE_EXCEPTION_ERROR
	}
	private ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	
	
	@Override
	protected void setup(Reducer<Text, Text, ImmutableBytesWritable, KeyValue>.Context context) throws IOException, InterruptedException {
		super.setup(context);
		cf=context.getConfiguration().get(BasicConstants.HBASE_COLUMN_FAMILY,"d");
	}
	
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		try {
			for (Text val : values) {
				String valStr=val.toString();
				String colSplit=context.getConfiguration().get(BasicConstants.HDFS_COLUMN_SPLIT,"\t");
				String[] colVal=valStr.split(colSplit);
				//写入到hfile文件中
				hKey.set(key.toString().getBytes());
				Set<KeyValue> hfileColumns=generateColumns(key.toString(), colVal);
				for (KeyValue hKv : hfileColumns) {
					context.write(hKey, hKv);
				}
			}
		} catch (Exception e) {
			context.getCounter(MONITOR.REDUCE_MOITOR.toString(), MONITOR.MONITOR_REDUCE_EXCEPTION_ERROR.toString()).increment(1);
			logger.error("reduce-Exception={}",e);
		}
	}
	
	

	public Set<KeyValue> generateColumns(String rowKey,String[] colVal){
		TreeSet<KeyValue> hfileColumns = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
		
		//gid
		String gidVal=StringUtils.isBlank(colVal[0])==true?StringUtils.EMPTY:colVal[0];
		KeyValue kv_gid = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.GID.getVal().getBytes(),
				gidVal.getBytes());
		hfileColumns.add(kv_gid);
		
		//gender
		String genderVal=StringUtils.isBlank(colVal[2])==true?StringUtils.EMPTY:colVal[2];
		KeyValue kv_gender = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.GENDER.getVal().getBytes(),
				genderVal.getBytes());
		hfileColumns.add(kv_gender);
		
		
		//age
		String ageVal=StringUtils.isBlank(colVal[3])==true?StringUtils.EMPTY:colVal[3];
		KeyValue kv_age = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.AGE.getVal().getBytes(),
				ageVal.getBytes());
		hfileColumns.add(kv_age);
		
		
		//area
		String areaVal=StringUtils.isBlank(colVal[4])==true?StringUtils.EMPTY:colVal[4];
		KeyValue kv_area = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.AREA.getVal().getBytes(),
				areaVal.getBytes());
		hfileColumns.add(kv_area);
		
		
		
		
		//house
		String houseVal=StringUtils.isBlank(colVal[5])==true?StringUtils.EMPTY:colVal[5];
		KeyValue kv_house = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.HOUSE.getVal().getBytes(),
				houseVal.getBytes());
		hfileColumns.add(kv_house);
		
		//car
		String carVal=StringUtils.isBlank(colVal[6])==true?StringUtils.EMPTY:colVal[6];
		KeyValue kv_car = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.CAR.getVal().getBytes(),
				carVal.getBytes());
		hfileColumns.add(kv_car);
		
		
		//has_child
		String hasChildVal=StringUtils.isBlank(colVal[7])==true?StringUtils.EMPTY:colVal[7];
		KeyValue kv_hasChild = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.HAS_CHILD.getVal().getBytes(),
				hasChildVal.getBytes());
		hfileColumns.add(kv_hasChild);
		
		//phone_brand
		String phoneBrandVal=StringUtils.isBlank(colVal[8])==true?StringUtils.EMPTY:colVal[8];
		KeyValue kv_phoneBrand = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.PHONE_BRAND.getVal().getBytes(),
				phoneBrandVal.getBytes());
		hfileColumns.add(kv_phoneBrand);
		
		
		//mon_dv
		String monDvVal=StringUtils.isBlank(colVal[9])==true?StringUtils.EMPTY:colVal[9];
		KeyValue kv_monDv = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.MON_DV.getVal().getBytes(),
				monDvVal.getBytes());
		hfileColumns.add(kv_monDv);
		
		
		//needs
		String needsVal=StringUtils.isBlank(colVal[10])==true?StringUtils.EMPTY:colVal[10];
		KeyValue kv_needs = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.NEEDS.getVal().getBytes(),
				needsVal.getBytes());
		hfileColumns.add(kv_needs);
		
		//prefs
		String prefsVal=StringUtils.isBlank(colVal[11])==true?StringUtils.EMPTY:colVal[11];
		KeyValue kv_prefs = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.PREFS.getVal().getBytes(),
				prefsVal.getBytes());
		hfileColumns.add(kv_prefs);
		
		//apps
		String appsVal=StringUtils.isBlank(colVal[12])==true?StringUtils.EMPTY:colVal[12];
		KeyValue kv_apps = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.APPS.getVal().getBytes(),
				appsVal.getBytes());
		hfileColumns.add(kv_apps);
		
		//last_online
		String lastOnLineVal=StringUtils.isBlank(colVal[5])==true?StringUtils.EMPTY:colVal[5];
		KeyValue kv_lastOnline = new KeyValue(rowKey.getBytes(), cf.getBytes(),
				BasicConstants.USER_BASE_INFO_FIELD.LAST_ONLINE.getVal().getBytes(),
				lastOnLineVal.getBytes());
		hfileColumns.add(kv_lastOnline);
		
		return hfileColumns;
	}
	
	
}
