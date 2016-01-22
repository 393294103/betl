/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2016年1月13日下午5:37:07
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.betl.mysql.core.product;

import java.io.File;
import java.io.FileOutputStream;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * @author Administrator
 *
 */
public class Test extends ClassLoader implements Opcodes {
	public static byte[] dump() throws Exception {

		ClassWriter cw = new ClassWriter(0);
		FieldVisitor fv;
		MethodVisitor mv;
		AnnotationVisitor av0;

		cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER, "org/objectweb/asm/demo/HelloWorld", null, "java/lang/Object", null);

		cw.visitSource("HelloWorld.java", null);

		{
			mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
			mv.visitCode();
			Label l0 = new Label();
			mv.visitLabel(l0);
			mv.visitLineNumber(13, l0);
			mv.visitVarInsn(ALOAD, 0);
			mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
			mv.visitInsn(RETURN);
			Label l1 = new Label();
			mv.visitLabel(l1);
			mv.visitLocalVariable("this", "Lorg/objectweb/asm/demo/HelloWorld;", null, l0, l1, 0);
			mv.visitMaxs(1, 1);
			mv.visitEnd();
		}
		{
			mv = cw.visitMethod(ACC_PUBLIC, "sayHello", "()V", null, null);
			mv.visitCode();
			Label l0 = new Label();
			mv.visitLabel(l0);
			mv.visitLineNumber(15, l0);
			mv.visitFieldInsn(GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
			mv.visitLdcInsn("Hello world!");
			mv.visitMethodInsn(INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V");
			Label l1 = new Label();
			mv.visitLabel(l1);
			mv.visitLineNumber(16, l1);
			mv.visitInsn(RETURN);
			Label l2 = new Label();
			mv.visitLabel(l2);
			mv.visitLocalVariable("this", "Lorg/objectweb/asm/demo/HelloWorld;", null, l0, l2, 0);
			mv.visitMaxs(2, 1);
			mv.visitEnd();
		}
		cw.visitEnd();

		return cw.toByteArray();
	}

	public static void main(String[] args) throws Exception {
		byte[] data = dump();
		File file = new File("D://HelloWorld.class");
		FileOutputStream fout = new FileOutputStream(file);
		fout.write(data);
		fout.close();
	}
}
