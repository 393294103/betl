/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月19日下午3:34:44
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr.model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

/**
 * @author zhl
 *
 */
public interface IMysqlModel extends Writable, DBWritable {

	public String formatHdfsStr();

}
