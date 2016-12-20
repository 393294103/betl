/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月19日下午5:28:28
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.mr.model;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import javax.tools.JavaCompiler.CompilationTask;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.mysql.mr.Constants;
import com.betl.mysql.mr.Mysql2Hdfs;
import com.betl.mysql.util.JavaStringObject;

/**
 * @author zhl
 *
 */
public class MysqlModelImplCode {
	private Logger logger = LoggerFactory.getLogger(getClass());
	private final String newLine = "\n";
	
	

	public String gengerate(Configuration conf) {
		String fieldStr = conf.get("mysql.table.columns");
		logger.debug("[main-fieldStr]\t{}", fieldStr);
		String[] colAndTypes = fieldStr.split(Constants.SQL_COLUMN_SPLIT_BY);

		StringBuilder code = new StringBuilder();
		/**
		 * 类的package、导入的类等
		 */
		code.append("package com.betl.mysql.mr.model;" + newLine);
		code.append("import java.io.DataInput;" + newLine);
		code.append("import java.io.DataOutput;" + newLine);
		code.append("import java.io.IOException;" + newLine);
		code.append("import java.sql.PreparedStatement;" + newLine);
		code.append("import java.sql.ResultSet;" + newLine);
		code.append("import java.sql.SQLException;" + newLine);
		code.append("import org.apache.hadoop.io.Text;" + newLine);
		code.append("public class NewsDoc implements IMysqlModel{" + newLine);

		/*
		 * private String url; 
		 * private String title; 
		 * private String content;
		 */
		
		
		
		for (String str : colAndTypes) {
			String[] ct = str.split(Constants.SQL_Type_SPLIT_BY);
			if (ct[1].toLowerCase().equals("string")) {
				code.append("private String " + ct[0] + ";" + newLine);
			}
		}

		code.append("public void write(PreparedStatement statement) throws SQLException {" + newLine);

		/*
		 * statement.setString(1, this.url); 
		 * statement.setString(2, this.title);
		 * statement.setString(3, this.content);
		 */

		int i = 1;
		for (String str : colAndTypes) {
			System.out.println(i);
			String[] ct = str.split(Constants.SQL_Type_SPLIT_BY);
			if (ct[1].toLowerCase().equals("string")) {
				code.append("statement.setString(" + i + ", this." + ct[0] + ");" + newLine);
			}
			i++;
		}

		code.append("}" + newLine);

		/*
		 * public void readFields(ResultSet resultSet) throws SQLException {
		 * this.url = resultSet.getString("url"); this.title =
		 * resultSet.getString("title"); this.content =
		 * resultSet.getString("content"); }
		 */

		code.append("public void readFields(ResultSet resultSet) throws SQLException {" + newLine);

		for (String str : colAndTypes) {
			String[] ct = str.split(Constants.SQL_Type_SPLIT_BY);
			if (ct[1].toLowerCase().equals("string")) {
				code.append("this." + ct[0] + " = resultSet.getString(\"" + ct[0] + "\");" + newLine);
			}
		}
		code.append("}" + newLine);

		/*
		 * public void write(DataOutput out) throws IOException {
		 * Text.writeString(out, this.url); Text.writeString(out, this.title);
		 * Text.writeString(out, this.content); }
		 */

		code.append("public void write(DataOutput out) throws IOException {" + newLine);

		for (String str : colAndTypes) {
			String[] ct = str.split(Constants.SQL_Type_SPLIT_BY);
			if (ct[1].toLowerCase().equals("string")) {
				code.append("Text.writeString(out, this." + ct[0] + ");" + newLine);
			}
		}
		code.append("}" + newLine);

		/*
		 * public void readFields(DataInput in) throws IOException { this.url =
		 * Text.readString(in); this.title = Text.readString(in); this.content =
		 * Text.readString(in); }
		 */
		code.append("public void readFields(DataInput in) throws IOException {" + newLine);

		for (String str : colAndTypes) {
			String[] ct = str.split(Constants.SQL_Type_SPLIT_BY);
			if (ct[1].toLowerCase().equals("string")) {
				code.append("this." + ct[0] + " = Text.readString(in);" + newLine);
			}
		}
		code.append("}" + newLine);

		/*
		 * @Override public String formatHdfsStr() { StringBuilder res=new
		 * StringBuilder(); res.append(this.url).append("\t")
		 * .append(this.title).append("\t") .append(this.content).append("\t");
		 * return res.toString(); }
		 */
		code.append("@Override" + newLine);
		code.append("public String formatHdfsStr() {" + newLine);
		code.append("StringBuilder res=new StringBuilder();" + newLine);
		code.append("res");
		for (String str : colAndTypes) {
			String[] ct = str.split(Constants.SQL_Type_SPLIT_BY);
			if (ct[1].toLowerCase().equals("string")) {
				code.append(".append(this.title).append(\"\\t\")" + newLine);
			}
		}
		code.append(";" + newLine);
		code.append("return res.toString();");
		code.append("}" + newLine);

		code.append("}" + newLine);
		System.out.println(code.toString());
		return code.toString();
	}

	
	
	public Class compile(String code) throws ClassNotFoundException, MalformedURLException{
		logger.debug("[compile-MysqlModelImplCode]\t{}",code);
		
		StringWriter writer = new StringWriter();// 内存字符串输出流 
	    PrintWriter out = new PrintWriter(writer); 
	    out.println(code); 
	    out.flush(); 
	    out.close(); 

		
		// 开始编译 
	    JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler(); 
	    JavaFileObject fileObject = new JavaStringObject("NewsDoc", writer.toString());
	   
		
	    String modelClassPath=Mysql2Hdfs.class.getResource("/").getPath();
	    
	    logger.debug("[compile-modelClassPath]\t{}",modelClassPath);
	    
	    
	    CompilationTask  task=javaCompiler.getTask(null, null, null,
	    		Arrays.asList("-d",modelClassPath), null, Arrays.asList(fileObject)); 
	    
	    boolean success=task.call(); 
	    
		Class clazz =null;
	    
	    if(!success){ 
	    	logger.debug("[compile-code-compile-flag]\t{}",success);
	       } 
	       else{ 
	    	logger.debug("[compile-code-compile-flag]\t{}",success);
	    	
	    	URL[] urls=new URL[]{new URL("file:/"+modelClassPath)}; 
	        URLClassLoader classLoader=new URLClassLoader(urls); 
	         clazz=classLoader.loadClass("com.betl.mysql.mr.model.NewsDoc"); 
	        logger.debug("[compile-clazz]\t{}",clazz);
	        return clazz;
	    }
		
		return null;
		
	}
	
	
	

}
