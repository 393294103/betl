/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月25日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr.model.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public  interface AbstractModeRecord {

	public static enum METHOD{
		write,readFields,formatHdfsStr
	}




	public abstract void write(PreparedStatement statement) throws SQLException;

	public abstract void readFields(ResultSet resultSet) throws SQLException;

	public abstract void write(DataOutput out) throws IOException;

	public abstract void readFields(DataInput in) throws IOException;

	public abstract String formatHdfsStr();

}
