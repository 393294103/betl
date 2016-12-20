/**
 * @Email:zhanghelin@geotmt.com
 * @Author:zhl
 * @Date:2016年12月15日下午3:59:06
 * @Copyright ZHL All Rights Reserved.
 */
package com.betl.common.parser;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * @author zhl
 *
 */
public class TestParser {
	
	public static void main(String[] args) throws IOException {
		 Document doc=Jsoup.connect("http://www.toutiao.com/a6363987714443149569/")
				 .get();
		
		 
		 System.out.println(doc.select("body").select("div.y-wrap").select("div.y-box,div.container")
				 .select("")
				 
				 
				 
				 .text());
		 
	}
	
	
	

}
