package com.getl.gstorm.core.bolt;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MsgScheme implements Scheme {
	
	private static final long serialVersionUID = 8423372426211017613L;

	public List<Object> deserialize(byte[] bytes) {
			try {
				String msg = new String(bytes, "UTF-8");
				String[] splits=msg.split("\t");
				return new Values(splits[0],splits[1],splits[2]); 
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return null;
	}


	public Fields getOutputFields() {
		return new Fields("url","title","content");
	}
}