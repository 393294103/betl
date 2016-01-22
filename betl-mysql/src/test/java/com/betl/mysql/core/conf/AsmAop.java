/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2016年1月13日下午4:52:38
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.betl.mysql.core.conf;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import com.betl.mysql.core.product.BeanTest;
import com.betl.option.ConfOption;

public class AsmAop extends ClassLoader implements Opcodes {

	private static void makeSetMethod(ClassWriter cw, String className, String fieldName, String fieldType) {
		MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, AsmHelp.setMethodName(fieldName), "(" + AsmHelp.packageWarpType(fieldType) + ")V", null, null);
		mv.visitCode();
		mv.visitVarInsn(Opcodes.ALOAD, 0);
		mv.visitVarInsn(Opcodes.ALOAD, 1);
		mv.visitFieldInsn(Opcodes.PUTFIELD, className, fieldName, AsmHelp.packageWarpType(fieldType));
		mv.visitInsn(Opcodes.RETURN);
		// mv1.visitMaxs(2, 2);
		mv.visitEnd();
	}

	private static void makeGetMethod(ClassWriter cw, String className, String fieldName, String fieldType) {

		MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, AsmHelp.getMethodName(fieldName), "()" + AsmHelp.packageWarpType(fieldType), null, null);
		mv.visitCode();
		mv.visitVarInsn(Opcodes.ALOAD, 0);
		mv.visitFieldInsn(Opcodes.GETFIELD, className, fieldName, AsmHelp.packageWarpType(fieldType));
		mv.visitInsn(ARETURN);
		// mv.visitMaxs(1, 1);
		mv.visitEnd();
	}

	private static void preparedStatementWrite(ClassWriter cw, String MethodName, String className, Map<String, String> nameType) {
		MethodVisitor mv1 = cw.visitMethod(Opcodes.ACC_PUBLIC, MethodName, "(Ljava/sql/PreparedStatement;)V", null, new String[] { "SQLException" });
		int i = 1;
		for (String key : nameType.keySet()) {
			mv1.visitCode();
			mv1.visitVarInsn(Opcodes.ALOAD, 0);
			mv1.visitVarInsn(Opcodes.ALOAD, 1);
			mv1.visitLdcInsn(i++);
			mv1.visitFieldInsn(GETSTATIC, className, key, AsmHelp.packageWarpType(nameType.get(key)));
			mv1.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/sql/PreparedStatement", AsmHelp.PreparedStatementMethod(nameType.get(key)), "(Ljava/lang/Integer;" + AsmHelp.packageWarpType(nameType.get(key)) + ")V");
		}
		mv1.visitInsn(Opcodes.RETURN);
		mv1.visitEnd();
	}

	private static void dataOutputWrite(ClassWriter cw, String MethodName, String className, Map<String, String> nameType) {
		MethodVisitor mv1 = cw.visitMethod(Opcodes.ACC_PUBLIC, MethodName, "(Ljava/io/DataOutput;)V", null, new String[] { "IOException" });
		for (String key : nameType.keySet()) {
			mv1.visitCode();
			mv1.visitVarInsn(Opcodes.ALOAD, 0);
			mv1.visitVarInsn(Opcodes.ALOAD, 1);
			mv1.visitFieldInsn(GETSTATIC, className, key, AsmHelp.packageWarpType(nameType.get(key)));
			mv1.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/hadoop/io/Text", "writeString", "(Ljava/io/DataOutput;" + AsmHelp.packageWarpType(nameType.get(key)) + ")V");
		}
		mv1.visitInsn(Opcodes.RETURN);
		mv1.visitEnd();
	}

	private static void readFieldsResultSet(ClassWriter cw, String MethodName, String className, Map<String, String> nameType) {
		MethodVisitor mv1 = cw.visitMethod(Opcodes.ACC_PUBLIC, MethodName, "(Ljava/sql/ResultSet;)V", null, new String[] { "SQLException" });

		for (String key : nameType.keySet()) {
			mv1.visitCode();
			mv1.visitVarInsn(Opcodes.ALOAD, 0);
			mv1.visitVarInsn(Opcodes.ALOAD, 1);
			mv1.visitFieldInsn(GETSTATIC, className, "\"" + key + "\"", AsmHelp.packageWarpType(nameType.get(key)));
			mv1.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/sql/ResultSet", "getString", "(" + AsmHelp.packageWarpType(nameType.get(key)) + ")L");
			mv1.visitFieldInsn(Opcodes.PUTFIELD, className, key, AsmHelp.packageWarpType(nameType.get(key)));
		}
		mv1.visitEnd();
	}

	private static void readFieldsDataInput(ClassWriter cw, String MethodName, String className, Map<String, String> nameType) {
		MethodVisitor mv1 = cw.visitMethod(Opcodes.ACC_PUBLIC, MethodName, "(Ljava/io/DataInput;)V", null, new String[] { "IOException" });

		for (String key : nameType.keySet()) {
			mv1.visitCode();
			mv1.visitVarInsn(Opcodes.ALOAD, 0);
			mv1.visitVarInsn(Opcodes.ALOAD, 1);
			// mv1.visitFieldInsn(GETSTATIC, className, key,
			// Test2.packageWarpType(nameType.get(key)));
			mv1.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/hadoop/io/Text", "readString", "(Ljava/io/DataOutput;)L");
			mv1.visitFieldInsn(Opcodes.PUTFIELD, className, key, AsmHelp.packageWarpType(nameType.get(key)));
		}
		mv1.visitEnd();
	}

	private static void tableToString(ClassWriter cw, String MethodName, String className, Map<String, String> nameType) {
		MethodVisitor mv1 = cw.visitMethod(Opcodes.ACC_PUBLIC, MethodName, "()Ljava/lang/String;", null, null);
		mv1.visitAnnotation("LOverride;", true);
		mv1.visitTypeInsn(NEW, "java/lang/StringBuilder"); 
		for (String key : nameType.keySet()) {
			mv1.visitFieldInsn(GETSTATIC, className,  key , AsmHelp.packageWarpType(nameType.get(key)));
			 mv1.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", 
					 "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;"); 
			 mv1.visitLdcInsn("\t");
			 mv1.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", 
					 "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;"); 
		}
		mv1.visitMethodInsn(INVOKEVIRTUAL, "java/lang/StringBuilder", 
				 "toString", "()Ljava/lang/String;"); 
		mv1.visitInsn(ARETURN);
		mv1.visitEnd();
	}
	
	
	
	public static byte[] buildTableClazz(String[] args) throws IOException {
		ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
		cw.visit(V1_5, ACC_PUBLIC + ACC_SUPER, "Table", null, "java/lang/Object", new String[] { "org/apache/hadoop/io/Writable", "org/apache/hadoop/mapred/lib/db/DBWritable" });

		// 空构造
		MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
		mv.visitVarInsn(ALOAD, 0);
		mv.visitMethodInsn(INVOKEINTERFACE, "java/lang/Object", "<init>", "()V");
		mv.visitInsn(RETURN);
		mv.visitMaxs(1, 1);
		mv.visitEnd();

		Configuration conf = ConfOption.getConf(args);
		String mysqlCols = conf.get("mysql.table.columns");

		//创建的类名字
		String clazzName="com/betl/mysql/core/model/Table";
		
		
		// 给这个类添加一些field
		Map<String, String> nameType = AsmHelp.colNameAndType(mysqlCols);
		for (String key : nameType.keySet()) {
			FieldVisitor fv = cw.visitField(ACC_PUBLIC, key, AsmHelp.packageWarpType(nameType.get(key)), null, null);
			fv.visitEnd();
			// 设置set方法
			makeSetMethod(cw, clazzName, key, nameType.get(key));
			// 设置get方法
			makeGetMethod(cw, clazzName, key, nameType.get(key));
		}

		preparedStatementWrite(cw, "write", clazzName, nameType);
		dataOutputWrite(cw, "write", clazzName, nameType);
		readFieldsResultSet(cw, "readFields", clazzName, nameType);
		readFieldsDataInput(cw, "readFields", clazzName, nameType);
		tableToString(cw, "toString", clazzName, nameType);
		
		byte[] code = cw.toByteArray();
		return code;
	}
	
	public static void main(String[] args) throws IOException {
		byte[] code=AsmAop.buildTableClazz(args);
		AsmAop loader = new AsmAop();
		Class<?> clazz = loader.defineClass(null, code, 0, code.length);
		
	}
	

}
