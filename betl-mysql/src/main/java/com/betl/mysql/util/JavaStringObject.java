/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月19日下午3:30:56
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.mysql.util;
import java.io.IOException; 
import java.net.URI; 
import javax.tools.SimpleJavaFileObject; 
public class JavaStringObject extends SimpleJavaFileObject { 
  private String code; 
  public JavaStringObject(String name, String code) { 
   //super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE); 
   super(URI.create(name+".java"), Kind.SOURCE); 
   this.code = code; 
  } 
  @Override 
  public  CharSequence  getCharContent(boolean  ignoreEncodingErrors)  throws  IOException 
  { 
   return code; 
  } 
}