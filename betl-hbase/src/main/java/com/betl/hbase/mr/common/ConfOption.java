/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月10日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.hbase.mr.helper.PreSplitHBaseHelper;


public class ConfOption {
	static Logger logger = LoggerFactory.getLogger(ConfOption.class);

	public static Options buildJobControllerOptions() {
		Options options = new Options();
		
		Option job = new Option("job", BasicConstants.MR_JOB_NAME, true,"job support MR_JOB_TYPE in [TAG_WEIGHT_JOB,CATE_WEIGHT_JOB,USER_INFO_JOB].");
		job.setRequired(true);
		options.addOption(job);
		
		Option inpath = new Option("input", BasicConstants.HDFS_INPUT_PATH, true,"hdfs input path.");
		inpath.setRequired(true);
		options.addOption(inpath);
		
		Option outpath = new Option("output", BasicConstants.HDFS_OUTPUT_PATH, true,"hdfs output path.");
		outpath.setRequired(true);
		options.addOption(outpath);
		
		Option reduces = new Option("reduces", BasicConstants.MAPREDUCE_JOB_REDUCES, true,"mapreduce.job.reduces.");
		reduces.setRequired(true);
		options.addOption(reduces);
		
		
	
		
		
		
		Option hcf = new Option("hcf", BasicConstants.HBASE_COLUMN_FAMILY, true,"hbase column family.");
		hcf.setRequired(false);
		options.addOption(hcf);
		
		OptionBuilder.withArgName("property=value");
		OptionBuilder.hasArgs(2);
		OptionBuilder.withValueSeparator();
		OptionBuilder.withDescription("use value for given property");
		options.addOption(OptionBuilder.create("D"));

		return options;
	}

	
	
	
	public static Options buildMrHTabHelpOptions() {
		Options options = new Options();
		Option htab = new Option("ht", BasicConstants.HBASE_TAB_NAME, true,"hbase tab name.");
		htab.setRequired(true);
		options.addOption(htab);
		Option regionNum = new Option("rn", BasicConstants.HBASE_REGION_NUM, true,"hbase region num,(rn 大于2,并且是2的n次方,方便均分!)");
		regionNum.setRequired(true);
		options.addOption(regionNum);
		Option cf = new Option("cf", BasicConstants.HBASE_COLUMN_FAMILY, true,"hbase column famaily.");
		cf.setRequired(true);
		options.addOption(cf);
		return options;
		}
	
	
	
	
	
	public static Map<String,String> getJobHelpHTabConf(String[] args) {
		Options options = buildMrHTabHelpOptions();
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(350);
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;

		try {
			// 处理Options和参数
			cmd = parser.parse(options, args);
			Integer numRegions=Integer.valueOf(cmd.getOptionValue(BasicConstants.HBASE_REGION_NUM));
			boolean flag=PreSplitHBaseHelper.checkNum(numRegions);
			if(!flag){
				throw new RuntimeException("hbase.region.num is unavailable,(rn 大于2,并且是2的n次方,方便均分!)");
			}
			
			
			
			
		} catch (Exception e) {
			logger.error("getConf-ParseException={}",e.getMessage());
			formatter.printHelp("args", options); // 如果发生异常，则打印出帮助信息
			System.exit(0);
		}
		Map<String,String> param=new HashMap<String,String>();
	
		param.put(BasicConstants.HBASE_TAB_NAME, cmd.getOptionValue(BasicConstants.HBASE_TAB_NAME));
		param.put(BasicConstants.HBASE_COLUMN_FAMILY, cmd.getOptionValue(BasicConstants.HBASE_COLUMN_FAMILY));
		param.put(BasicConstants.HBASE_REGION_NUM, cmd.getOptionValue(BasicConstants.HBASE_REGION_NUM));
	
		return param;
	}
	

	
	
	public static boolean isValidDate(String str) {
	      boolean convertSuccess=true;
	      // 指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
	       SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
	       try {
	    	   // 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
	          format.setLenient(false);
	          format.parse(str);
	       } catch (ParseException e) {
	          // e.printStackTrace();
	    	   // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
	           convertSuccess=false;
	       } 
	       return convertSuccess;
	}
	
	public static String getConfDateStr(String input){
		String newPath=StringUtils.EMPTY;
		if(input.substring(input.length()-1).equals("/")){
			newPath=input.substring(0, input.length()-1);
		}else{
			newPath=input;
		}
		
		newPath.lastIndexOf("/");
		String dateStr=newPath.substring(newPath.lastIndexOf("/")+1, newPath.length());
		return dateStr;
	}
	
	
	


}
