/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015年11月3日下午2:36:08
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.betl.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsUtil {
	// 2. 声明一个Logger，这个是static的方式，我比较习惯这么写。
	private  final Logger logger = LoggerFactory.getLogger(getClass());

	public FileSystem hdfs;

	public HdfsUtil(FileSystem hdfs) throws IOException, URISyntaxException {
		this.hdfs = hdfs;
	}

	public boolean existFsFile(String file) throws IOException {
		Path path = new Path(file);
		return hdfs.exists(path);
	}

	public long fileSize(String file) throws IOException {
		Path path = new Path(file);
		return hdfs.getContentSummary(path).getLength();
	}

	public List<String> listFolderName(String pathUri) throws FileNotFoundException, IOException {
		List<String> childPaths = new ArrayList<String>();
		Path path = new Path(pathUri);
		FileStatus status[];
		int counter = 0;
		status = hdfs.listStatus(path);
		for (int i = 0; i < status.length; i++) {
			if (status[i].isDirectory()) {
				String childPath = status[i].getPath().toString();
				String folder = childPath.substring(childPath.lastIndexOf("/") + 1, childPath.length());
				childPaths.add(folder);
				counter++;
			}
		}
		logger.debug("{}{}\tchild folder size :{}", hdfs.getUri(), pathUri, counter);
		return childPaths;
	}

	public List<String> listFiles(String pathUri) throws FileNotFoundException, IOException {
		List<String> listFiles = new ArrayList<String>();
		Path path = new Path(pathUri);
		FileStatus status[];
		int counter = 0;
		status = hdfs.listStatus(path);
		for (int i = 0; i < status.length; i++) {
			if (status[i].isFile()) {
				String childPath = status[i].getPath().toString();
				listFiles.add(childPath);
				counter++;
			}
		}
		logger.debug("{}{}\tlist file size : {}", hdfs.getUri(), pathUri, counter);
		return listFiles;
	}

	public boolean deletePath(String path) throws IOException {
		Path fsPath = new Path(path);
		@SuppressWarnings("deprecation")
		boolean isDeleted = hdfs.delete(fsPath);
		logger.debug("delete file or path {}/{}", hdfs.getUri(), path);
		return isDeleted;
	}

}
