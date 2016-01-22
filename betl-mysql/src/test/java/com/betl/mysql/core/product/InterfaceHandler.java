/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2016年1月14日上午10:05:47
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.betl.mysql.core.product;

/**
 * @author Administrator
 *
 */
	import java.io.FileOutputStream;
	import java.lang.reflect.Method;

	import org.objectweb.asm.ClassWriter;
	import org.objectweb.asm.Label;
	import org.objectweb.asm.MethodVisitor;
	import org.objectweb.asm.Opcodes;

	public class InterfaceHandler extends ClassLoader implements Opcodes
	{
	    public static void main(final String args[]) throws Exception
	    {
	        ISayHello iSayHello = (ISayHello)MakeClass(ISayHello.class);
	        
	        iSayHello.MethodA();
	        
	        iSayHello.MethodB();
	        
	        iSayHello.Abs();
	        
	    }
	    
	    public static Object MakeClass(Class clazz) throws Exception
	    {
	        String name = clazz.getSimpleName();
	        String className = name + "$imp";
	        
	        String Iter = clazz.getName().replaceAll("\\.", "/");
	        
	        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
	        cw.visit(V1_5,
	            ACC_PUBLIC + ACC_SUPER,
	            className,
	            null,
	            "java/lang/Object",
	            new String[] {Iter});
	        
	        //空构造
	        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC,
	            "<init>",
	            "()V",
	            null,
	            null);
	        mv.visitVarInsn(ALOAD, 0);
	        mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
	        mv.visitInsn(RETURN);
	        mv.visitMaxs(1, 1);
	        mv.visitEnd();
	        
	        Method[] methods = clazz.getMethods();
	        for (Method method : methods)
	        {
	            MakeMethod(cw, method.getName(), className);
	        }
	        
	        cw.visitEnd();
	        /*
	         * 写入文件
	         */
	        byte[] code = cw.toByteArray();
	        FileOutputStream fos = new FileOutputStream(className);
	        fos.write(code);
	        fos.close();
	        
	        /*
	         * 从文件加载类
	         */
	        InterfaceHandler loader = new InterfaceHandler();
	        Class exampleClass = loader.defineClass(className,
	            code,
	            0,
	            code.length);
	        
	        /*
	         * 反射生成实例
	         */
	        Object obj = exampleClass.getConstructor(null).newInstance(null);
	        
	        return obj;
	    }
	    
	    private static void MakeMethod(ClassWriter cw, String MethodName, String className)
	    {
	        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC,
	            MethodName,
	            "()V",
	            null,
	            null);
	        mv.visitCode();
	        Label l0 = new Label();
	        mv.visitLabel(l0);
	        mv.visitLineNumber(8, l0);
	        mv.visitFieldInsn(GETSTATIC,
	            "java/lang/System",
	            "out",
	            "Ljava/io/PrintStream;");
	        mv.visitLdcInsn("调用方法 [" + MethodName + "]");
	        mv.visitMethodInsn(INVOKEVIRTUAL,
	            "java/io/PrintStream",
	            "println",
	            "(Ljava/lang/String;)V");
	        Label l1 = new Label();
	        mv.visitLabel(l1);
	        mv.visitLineNumber(9, l1);
	        mv.visitInsn(RETURN);
	        Label l2 = new Label();
	        mv.visitLabel(l2);
	        mv.visitLocalVariable("this",
	            "L" + className + ";",
	            null,
	            l0,
	            l2,
	            0);
	        mv.visitMaxs(2, 1);
	        mv.visitEnd();
	    }
	}