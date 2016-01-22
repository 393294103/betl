/**
 * @Email:1768880751@qq.com
 * @Author:zhl
 * @Date:2016年1月22日下午5:21:03
 * @Copyright ZHL All Rights Reserved.
 */
package com.getl.gstorm.bolt;

import com.getl.gstorm.common.CommonUtil;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Administrator
 *
 */
public class MsgSplitBolt extends BaseBasicBolt{
    
   
    public void execute(Tuple input, BasicOutputCollector collector) {
         String url = (String) input.getString(0);
         String title = CommonUtil.replaceBlank((String) input.getString(1));
         String content =CommonUtil.replaceBlank((String) input.getString(2));
         collector.emit(new Values(url, title,content));
    }
    
 
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url","title","content"));
    }
}
