/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015年12月21日下午6:33:52
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.getl.gstorm.core.bolt;

import com.getl.gstorm.core.common.CommonUtil;

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
