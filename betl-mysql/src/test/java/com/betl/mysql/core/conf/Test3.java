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

import org.apache.hadoop.conf.Configuration;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import com.betl.common.core.option.ConfOption;

/**
 * @author Administrator
 *
 */
public class Test3 extends ClassLoader implements Opcodes {

	static int i = 0;
	static int j = 0;

	private static void makeSetMethod(ClassWriter cw, String MethodName, String className, String fieldName) {
		MethodVisitor mv1 = cw.visitMethod(Opcodes.ACC_PUBLIC, MethodName, "(Ljava/lang/String;)V", null, null);
		mv1.visitCode();
		mv1.visitVarInsn(Opcodes.ALOAD, 0);
		// mv1.visitVarInsn(Opcodes.ALOAD, 1);

		mv1.visitVarInsn(Opcodes.ALOAD, 1);

		mv1.visitFieldInsn(Opcodes.PUTFIELD, "Table", "name", "Ljava/lang/String;");
		mv1.visitInsn(Opcodes.RETURN);
		mv1.visitMaxs(2, 2);
		mv1.visitEnd();
	}

	private static void makeGetMethod(ClassWriter cw, String MethodName, String className, String fieldName) {
		MethodVisitor mv1 = cw.visitMethod(Opcodes.ACC_PUBLIC, MethodName, "(Ljava/lang/String;Ljava/lang/String;)V", null, null);
		mv1.visitCode();
		mv1.visitVarInsn(Opcodes.ALOAD, 0);
		mv1.visitVarInsn(Opcodes.ALOAD, 1);

		mv1.visitFieldInsn(Opcodes.PUTFIELD, "Table", "url", "Ljava/lang/String;");
		mv1.visitCode();
		mv1.visitVarInsn(Opcodes.ALOAD, 0);
		mv1.visitVarInsn(Opcodes.ALOAD, 2);
		mv1.visitFieldInsn(Opcodes.PUTFIELD, "Table", "title", "Ljava/lang/String;");
		mv1.visitInsn(Opcodes.RETURN);
		mv1.visitMaxs(2, 2);
		mv1.visitEnd();
	}

	
	
	
	private static void write(ClassWriter cw, String MethodName, String className, String fieldName) {
		MethodVisitor mv1 = cw.visitMethod(Opcodes.ACC_PUBLIC, MethodName, "(Ljava/sql/PreparedStatement;)V", null, new String[]{"SQLException"});
		mv1.visitCode();
		
		mv1.visitVarInsn(Opcodes.ALOAD, 0);
		mv1.visitVarInsn(Opcodes.ALOAD, 1);
		
		//mv1.visitVarInsn(Opcodes.ALOAD, 0);
		//mv1.visitVarInsn(Opcodes.ALOAD, 2);
		
		//mv1.visitVarInsn(Opcodes.ALOAD, 0);
		//mv1.visitVarInsn(Opcodes.ALOAD, 3);
		mv1.visitLdcInsn(1);
		//mv1.visitLdcInsn("url");
		// GETSTATIC, PUTSTATIC, GETFIELD or PUTFIELD.
		mv1.visitFieldInsn(GETSTATIC,"Table", "url", "Ljava/lang/String;");
		
		mv1.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/sql/PreparedStatement", "setString","(Ljava/lang/Integer;Ljava/lang/String;)V");  
		
		
		mv1.visitInsn(Opcodes.RETURN);
		mv1.visitMaxs(2, 2);
		mv1.visitEnd();
	}
	
	
	private static void readFieldsResultSet(ClassWriter cw, String MethodName, String className) {
		MethodVisitor mv1 = cw.visitMethod(Opcodes.ACC_PUBLIC, MethodName, "(Ljava/sql/ResultSet;)V", null, new String[] { "SQLException" });
		
			mv1.visitCode();
			mv1.visitVarInsn(Opcodes.ALOAD, 0);
			mv1.visitVarInsn(Opcodes.ALOAD, 1);
			mv1.visitFieldInsn(GETSTATIC, className, "\"url\"", AsmHelp.packageWarpType("String"));
			mv1.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/sql/ResultSet", "getString", "(" + AsmHelp.packageWarpType("String") + ")L");
			
			//mv1.visitJumpInsn(GOTO, end);
			/*mv1.visitVarInsn(Opcodes.ALOAD, 0);
			mv1.visitVarInsn(Opcodes.ALOAD, 1);*/
			mv1.visitFieldInsn(Opcodes.PUTFIELD, className, "url", AsmHelp.packageWarpType("String"));
		
		mv1.visitEnd();
	}
	
	
	public static void main(String[] args) throws IOException {
		
		
		
		Configuration conf = ConfOption.getConf(args);
		// System.out.println(conf.get("load.to.hive"));
		String mysqlCols = conf.get("mysql.table.columns");

		String[] tabCols = mysqlCols.split(",");
		for (String col : tabCols) {
			System.out.println(col);
		}

		ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
		cw.visit(V1_5, ACC_PUBLIC + ACC_SUPER, "Table", null, "java/lang/Object", new String[] { "org/apache/hadoop/io/Writable", "org/apache/hadoop/mapred/lib/db/DBWritable" });

		// 空构造
		MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
		mv.visitVarInsn(ALOAD, 0);
		mv.visitMethodInsn(INVOKEINTERFACE, "java/lang/Object", "<init>", "()V");
		mv.visitInsn(RETURN);
		mv.visitMaxs(1, 1);
		mv.visitEnd();

		// 给这个类添加一些field

		// 生成String name字段
		FieldVisitor fv = cw.visitField(ACC_PUBLIC, "url", "Ljava/lang/String;", null, null);
		// AnnotationVisitor av = fv.visitAnnotation("LNotNull;", true);
		// av.visit("value", "");
		// av.visitEnd();
		fv.visitEnd();

		// 生成String name字段
		FieldVisitor fv2 = cw.visitField(ACC_PUBLIC, "title", "Ljava/lang/String;", null, null);
		// AnnotationVisitor av2 = fv.visitAnnotation("LNotNull;", true);
		// av2.visit("value", "");
		// av2.visitEnd();
		fv2.visitEnd();

		// 生成方法
		i = 8;
		j = 9;

		makeSetMethod(cw, "setUrl", "table", "url");

		makeGetMethod(cw, "GetUrl", "table", "url");
		
		write(cw, "write", "table", "url");
		
		readFieldsResultSet(cw,"test","Table");
		

		byte[] code = cw.toByteArray();
		FileOutputStream fos = new FileOutputStream("D://Table.class");
		fos.write(code);
		fos.close();

	}

}
