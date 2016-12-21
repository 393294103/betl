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
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.betl.mysql.conf.ConfigHelper;
import com.betl.mysql.util.JavaStringObject;

/**
 * @author zhl
 *
 */
public class ModelRecordImplCode {
	private Logger logger = LoggerFactory.getLogger(getClass());
	private final String newLine = "\n";
	private URLClassLoader classLoader;
	
	private ConfigHelper cf;
	
	public ModelRecordImplCode(ConfigHelper cf){
		this.cf=cf;
	}

	public String gengerate() {
		Configuration conf=cf.getConf();
		String fieldStr = conf.get("mysql.table.columns");
		logger.debug("[main-fieldStr]\t{}", fieldStr);
		String[] cols =cf.mysqlColumns();

		StringBuilder code = new StringBuilder();
		/**
		 *类的package、导入的类等
		  package com.betl.mysql.mr.model;
			import java.io.DataInput;
			import java.io.DataOutput;
			import java.io.IOException;
			import java.sql.PreparedStatement;
			import java.sql.ResultSet;
			import java.sql.SQLException;
			import java.util.regex.Matcher;
			import java.util.regex.Pattern;
			import org.apache.hadoop.io.Text;
			import org.apache.hadoop.io.Writable;
			import org.apache.hadoop.mapred.lib.db.DBWritable;
			public class ModelRecordImpl implements IModelRecord{
		 */
		code.append("package com.betl.mysql.mr.model;" + newLine);
		code.append("import java.io.DataInput;" + newLine);
		code.append("import java.io.DataOutput;" + newLine);
		code.append("import java.io.IOException;" + newLine);
		code.append("import java.sql.PreparedStatement;" + newLine);
		code.append("import java.sql.ResultSet;" + newLine);
		code.append("import java.sql.SQLException;" + newLine);
		code.append("import org.apache.hadoop.io.Text;" + newLine);
		code.append("public class ModelRecordImpl implements IModelRecord{" + newLine);

		/**字段信息
		 * private String url; 
		 * private String title; 
		 * private String content;
		 */
		for (String col : cols) {
			code.append("public String "+col+ ";" + newLine);
		}

		
		
		/**
		 * public void write(PreparedStatement statement) throws SQLException {
		 * statement.setString(1, this.url); 
		 * statement.setString(2, this.title);
		 * statement.setString(3, this.content);}
		 */
		code.append("public void write(PreparedStatement statement) throws SQLException {" + newLine);
		int i = 1;
		for (String col : cols) {
			code.append("statement.setString(" + i + ", this." + col + ");" + newLine);
			i++;
		}
		code.append("}" + newLine);

		/**
		 * public void readFields(ResultSet resultSet) throws SQLException {
		 * this.url = resultSet.getString("url"); this.title =
		 * resultSet.getString("title"); this.content =
		 * resultSet.getString("content"); }
		 */
		code.append("public void readFields(ResultSet resultSet) throws SQLException {" + newLine);
		for (String col : cols) {
			code.append("this." + col + " = resultSet.getString(\"" + col + "\");" + newLine);
		}
		code.append("}" + newLine);

		/**
		 * public void write(DataOutput out) throws IOException {
		 * Text.writeString(out, this.url); Text.writeString(out, this.title);
		 * Text.writeString(out, this.content); }
		 */
		code.append("public void write(DataOutput out) throws IOException {" + newLine);
		for (String col : cols) {
			code.append("Text.writeString(out, this." + col + ");" + newLine);
		}
		code.append("}" + newLine);

		/**
		 * public void readFields(DataInput in) throws IOException { this.url =
		 * Text.readString(in); this.title = Text.readString(in); this.content =
		 * Text.readString(in); }
		 */
		code.append("public void readFields(DataInput in) throws IOException {" + newLine);
		for (String col : cols) {
			code.append("this." + col + " = Text.readString(in);" + newLine);
		}
		code.append("}" + newLine);

		/**
		 * @Override public String formatHdfsStr() { 
		 * StringBuilder res=newStringBuilder(); 
		 * res.append(this.url).append("\t")
		 * .append(this.title).append("\t") 
		 * .append(this.content).append("\t");
		 * return res.toString(); }
		 */
		code.append("@Override" + newLine);
		code.append("public String formatHdfsStr() {" + newLine);
		code.append("StringBuilder res=new StringBuilder();" + newLine);
		code.append("res");
		for (String col : cols) {
			code.append(".append(this."+col+").append(\""+conf.get("hdfs.columns.split")+"\")" + newLine);
		}
		code.append(";" + newLine);
		code.append("return res.toString();");
		code.append("}" + newLine);
		code.append("}" + newLine);
		return code.toString();
	}
	
	@SuppressWarnings("rawtypes")
	public Class gengerateClass(String modelClassPath) throws ClassNotFoundException, MalformedURLException{
		Class clazz =null;
		//编译成功，并获取实现类
    	URL[] urls=new URL[]{new URL("file:/"+modelClassPath)}; 
        classLoader = new URLClassLoader(urls); 
         clazz=classLoader.loadClass("com.betl.mysql.mr.model.ModelRecordImpl"); 
        logger.debug("[compile-clazz]\t{}",clazz);
        return clazz;
	}
	
	public String compile(String code) throws ClassNotFoundException, MalformedURLException{
		logger.debug("[compile-ModelRecordImplCode]\t{}",code);
		StringWriter writer = new StringWriter();// 内存字符串输出流 
	    PrintWriter out = new PrintWriter(writer); 
	    out.println(code); 
	    out.flush(); 
	    out.close(); 
		
		// 开始编译 
	    JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler(); 
	    JavaFileObject fileObject = new JavaStringObject("ModelRecordImpl", writer.toString());
	   
		//获取项目根路径
	    String modelClassPath=ModelRecordImplCode.class.getResource("/").getPath();
	    logger.debug("[compile-modelClassPath]\t{}",modelClassPath);
	    
	    //开始编译到指定的目录下
	    CompilationTask  task=javaCompiler.getTask(null, null, null,
	    		Arrays.asList("-d",modelClassPath), null, Arrays.asList(fileObject)); 
	    boolean success=task.call(); 
	    logger.debug("[compile-code-compile-flag]\t{}",success);
	    return success?modelClassPath:null;
	}
	
	
	
	
	
	
	
	

}
