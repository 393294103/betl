/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.product;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class BeanTest extends ClassLoader implements Opcodes {

	

	public static void main(String[] args) throws Exception {
		
		/********************************class***********************************************/

		ClassWriter cw = new ClassWriter(0);
		cw.visit(V1_6, ACC_PUBLIC, "com/pansoft/espdb/bean/Person", null, "java/lang/Object", null);
		
		
		
		/*********************************constructor**********************************************/
		
		MethodVisitor mw = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null,null);
		mw.visitVarInsn(ALOAD, 0);
		mw.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
		mw.visitInsn(RETURN);
		mw.visitMaxs(1, 1);
		mw.visitEnd();
		
		
		/*************************************field******************************************/
	
		
		FieldVisitor  fv = cw.visitField(ACC_PUBLIC, "name", "Ljava/lang/String;", null, null);
		AnnotationVisitor  av = fv.visitAnnotation("LNotNull;", true);
		av.visit("value", "abc");
		av.visitEnd();
		fv.visitEnd();

		

		
		
		
		
		
		
		
		
		/***********************************generate and load********************************************/
		
		byte[] code = cw.toByteArray();
		
		BeanTest loader = new BeanTest();
		Class<?> clazz = loader.defineClass(null, code, 0, code.length);
		
		
		/***********************************test********************************************/
		
		
		
		
		Object beanObj = clazz.getConstructor().newInstance();
		
		System.out.println(clazz.getField("name"));
		clazz.getField("name").set(beanObj, "zhangjg");
		
		
		
		
		String nameString = (String) clazz.getField("name").get(beanObj);
		System.out.println("filed value : " + nameString);
		
		

		
		
		
		
		
		
		
		/*String annoVal = clazz.getField("name").getAnnotation(NotNull.class).value();
		System.out.println("annotation value: " + annoVal);*/
		
	}
}