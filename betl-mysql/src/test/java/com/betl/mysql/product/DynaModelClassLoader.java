/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.product;

/**
 * @author Administrator
 *
 */
import java.io.File; 
import java.io.FileOutputStream; 
import java.io.IOException; 
import java.lang.reflect.InvocationTargetException; 
import java.net.URI; 

import org.apache.commons.lang.StringUtils; 
import org.objectweb.asm.ClassWriter; 
import org.objectweb.asm.MethodVisitor; 
import org.objectweb.asm.Opcodes; 
import org.objectweb.asm.Type; 


public class DynaModelClassLoader extends ClassLoader implements Opcodes { 

public Class getIntModelClass(String className, String[] fields) 
throws IllegalArgumentException, SecurityException, 
IllegalAccessException, InvocationTargetException, IOException { 
ClassWriter cw = new ClassWriter(0); 
Class exampleClass; 
cw.visit(V1_1, ACC_PUBLIC, className, null, "java/lang/Object", null); 
// creates a MethodWriter for the (implicit) constructor 
MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, 
null); 
// pushes the 'this' variable 
mv.visitVarInsn(ALOAD, 0); 
// invokes the super class constructor 
mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V"); 
mv.visitInsn(RETURN); 
// this code uses a maximum of one stack element and one local 
// variable 
mv.visitMaxs(1, 1); 
mv.visitEnd(); 
// fields = new String[]{"dept1S", "dept2S", "dept3S", "dept4S", 
// "dept5S"}; 
for (int i = 0; i < fields.length; i++) { 
String field = fields[i]; 
String setMd = "set" + StringUtils.capitalize(field); 
String getMd = "get" + StringUtils.capitalize(field); 
cw.visitField(ACC_PRIVATE, field, "I", null, new Integer(0)) 
.visitEnd();// int dept1S=0; 

mv = cw.visitMethod(ACC_PUBLIC, getMd, "()I", null, null); 
mv.visitCode(); 
mv.visitVarInsn(ALOAD, 0); 
mv.visitFieldInsn(GETFIELD, className, field, "I"); 
mv.visitInsn(IRETURN); 
mv.visitMaxs(1, 1); 
// mw.visitInsn(RETURN); 
// this code uses a maximum of two stack elements and two local 
// variables 
mv.visitEnd(); 
cw.visitEnd(); 

mv = cw.visitMethod(ACC_PUBLIC, setMd, "(I)V", null, null); 
mv.visitCode(); 
mv.visitVarInsn(ALOAD, 0); 
mv.visitVarInsn(ILOAD, 1); 
mv.visitFieldInsn(PUTFIELD, className, field, "I"); 
mv.visitInsn(RETURN); 
mv.visitMaxs(2, 2); 
// mw.visitInsn(RETURN); 
// this code uses a maximum of two stack elements and two local 
// variables 
mv.visitEnd(); 
} 
String[] strs = new String[]{"code"}; 
for (int i = 0; i < strs.length; i++) { 
String setMd = "set" + StringUtils.capitalize(strs[i]); 
String getMd = "get" + StringUtils.capitalize(strs[i]); 
cw.visitField(ACC_PRIVATE, strs[i], "Ljava/lang/String;", null, 
null).visitEnd();// int dept1S=0; 

mv = cw.visitMethod(ACC_PUBLIC, getMd, "()Ljava/lang/String;", null, 
null); 
mv.visitCode(); 
mv.visitVarInsn(ALOAD, 0); 
mv 
.visitFieldInsn(GETFIELD, className, strs[i], 
"Ljava/lang/String;"); 
mv.visitInsn(ARETURN); 
mv.visitMaxs(1, 1); 
mv.visitEnd(); 
cw.visitEnd(); 

mv = cw.visitMethod(ACC_PUBLIC, setMd, "(Ljava/lang/String;)V", 
null, null); 
mv.visitCode(); 
mv.visitVarInsn(ALOAD, 0); 
mv.visitVarInsn(ALOAD, 1); 
mv 
.visitFieldInsn(PUTFIELD, className, strs[i], 
"Ljava/lang/String;"); 
mv.visitMaxs(2, 2); 
mv.visitInsn(RETURN); 
mv.visitEnd(); 
} 

cw.visitEnd(); 
byte[] code = cw.toByteArray(); 
// // �ѱ���õ�.class�ļ��ŵ�����Ӧ�İ����� 
// д.class"WebRoot/WEB-INF/classes/" + 
FileOutputStream fos = new FileOutputStream("bin/"+className + ".class"); 
fos.write(code); 
fos.close(); 
String classFile = className.replaceAll("/", "."); 
System.out.println(classFile); 

DynaModelClassLoader loader = new DynaModelClassLoader(); 
exampleClass = loader.defineClass(classFile, code, 0, code.length); 
// exampleClass.getMethods()[0].invoke(null, new Object[] { null }); 
return exampleClass; 
} 



// /* (non-Javadoc) 
// * @see java.lang.ClassLoader#findClass(java.lang.String) 
// */ 
// protected Class findClass(String name) throws ClassNotFoundException { 
// // TODO Auto-generated method stub 
// return super.findClass(name); 
// } 
   
public void testClass(String name, String[] fields) throws InstantiationException, NoSuchMethodException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, IOException{ 
Class cls = null; 
String clsName=name.replaceAll("/","."); 
Class cls2=getIntModelClass(name,fields); 
Object wlo2 = cls2.newInstance(); 
cls = findLoadedClass(clsName); 

if(null==cls){ 
try { 
cls=getIntModelClass(name,fields); 
Object wlo = cls.newInstance(); 
// asm.WorkLoadObject wo=new asm.WorkLoadObject(); 
cls = wlo.getClass(); 
// cls.getMethods()[0].invoke(null, new Object[]{null}); 
java.lang.reflect.Method setMethod = cls.getMethod("setDept1S", 
new Class[]{int.class}); 
setMethod.invoke(wlo, new Object[]{new Integer(10)}); 
java.lang.reflect.Method getMethod = cls.getMethod("getDept1S", 
new Class[]{}); 
System.out.println(getMethod.invoke(wlo, new Object[]{})); 
setMethod = cls.getMethod("setCode", new Class[]{String.class}); 
setMethod.invoke(wlo, new Object[]{new String("z11")}); 
getMethod = cls.getMethod("getCode", new Class[]{}); 
System.out.println(getMethod.invoke(wlo, new Object[]{})); 
System.out.println(Type.INT_TYPE.getDescriptor()); 
} catch (IllegalArgumentException e) { 
// TODO Auto-generated catch block 
e.printStackTrace(); 
} catch (SecurityException e) { 
// TODO Auto-generated catch block 
e.printStackTrace(); 
} catch (IllegalAccessException e) { 
// TODO Auto-generated catch block 
e.printStackTrace(); 
} catch (InvocationTargetException e) { 
// TODO Auto-generated catch block 
e.printStackTrace(); 
} catch (IOException e) { 
// TODO Auto-generated catch block 
e.printStackTrace(); 
} 
}else{ 
System.out.println(name+" has been found."); 
} 

} 


public static void main(final String args[]) throws Exception { 
DynaModelClassLoader hd = new DynaModelClassLoader(); 
String className = "WorkLoadObject"; 
hd.testClass(className,new String []{"dept1S"}); 
}
}
