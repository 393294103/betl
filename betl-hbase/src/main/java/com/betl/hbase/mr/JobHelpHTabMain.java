/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月12日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.hbase.mr;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.ParseException;

import com.betl.hbase.mr.common.BasicConstants;
import com.betl.hbase.mr.common.ConfOption;
import com.betl.hbase.mr.helper.PreSplitHBaseHelper;

public class JobHelpHTabMain {
	public static void main(String[] args) throws ParseException, IOException {
		Map<String,String> param=ConfOption.getJobHelpHTabConf(args);
		String hbaseCreate=PreSplitHBaseHelper.showCreateHTab(
				param.get(BasicConstants.HBASE_TAB_NAME), 
				Integer.valueOf(param.get(BasicConstants.HBASE_REGION_NUM)));
		System.out.println("HBASE_CREATE_SCRIPT\n"+hbaseCreate);
	}
}
