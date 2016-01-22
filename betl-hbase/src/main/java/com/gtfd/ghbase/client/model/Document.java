
package com.gtfd.ghbase.client.model;

import com.gtfd.ghbase.client.util.RedMD5Util;


/**
 * Copyright:   Copyright (c)2015
 * Company:     redhadoop.com
 * @version:    v1.0
 * Create at:   2015Âπ?Êú?5Êó•‰∏ãÂç?:35:52
 * Description:
 *
 * Modification History:
 * Date    Author                        Version     Description
 * ----------------------------------------------------------------- 
 * 2015Âπ?Êú?5Êó?zhanghelin@redhadoop.com         1.0       1.0 Version
 */

public class Document {
	private String uid;
	private String url;
	private String title;
	private String content;
	
	public Document(){}
	
	public Document(String url,String title,String content){
		this.uid=RedMD5Util.generate(url);
		this.url=url;
		this.title=title;
		this.content=content;
	}
	
	/**
	 * @return the uid
	 */
	public String getUid() {
		return uid;
	}
	/**
	 * @param uid the uid to set
	 */
	public void setUid(String uid) {
		this.uid = uid;
	}
	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}
	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.uid=RedMD5Util.generate(url);
		this.url = url;
	}
	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}
	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}
	/**
	 * @return the content
	 */
	public String getContent() {
		return content;
	}
	/**
	 * @param content the content to set
	 */
	public void setContent(String content) {
		this.content = content;
	}
	

}
