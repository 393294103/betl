/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2017年5月25日
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr.model.v2;

import java.io.DataOutput;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.hadoop.io.Text;

public class ModeRecordInterceptor implements MethodInterceptor {
	private List<String> fields=new ArrayList<String>();
	public ModeRecordInterceptor(List<String> fields){
		this.fields=fields;
	}	

	

	@Override
	public Object intercept(Object obj, Method method, Object[] args,
			MethodProxy proxy) throws Throwable {
		Object ret = null;  
		String name = method.getName();
		//public abstract void write(PreparedStatement statement) throws SQLException
		boolean flagWritePs=(AbstractModeRecord.METHOD.write.toString().equals(name))&&(args[0] instanceof PreparedStatement);
		//public abstract void write(DataOutput out) throws IOException
		boolean flagWriteDop=(AbstractModeRecord.METHOD.write.toString().equals(name))&&(args[0] instanceof DataOutput);
		//public abstract void readFields(ResultSet resultSet) throws SQLException;
		boolean flagReadFieldsRs=(AbstractModeRecord.METHOD.readFields.toString().equals(name))&&(args[0] instanceof ResultSet);
		
		
		
		if(flagWritePs){
			int i=1;
			PreparedStatement ps=(PreparedStatement) args[0];
			for (String field : fields) {
			ps.setString(i,field);
			i++;
			}
		}
		
		if(flagWriteDop){
				DataOutput dop=(DataOutput) args[0];
				for (String field : fields) {
					Text.writeString(dop, field);
				}
		}
		
		
		if(flagReadFieldsRs){
			ResultSet rs=(ResultSet) args[0];
			
			
		}
		
		return ret;
	}

	
	
}
