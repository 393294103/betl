/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015年12月23日下午5:24:42
 * @Desc:
 * @Copyright (c) 2014, 北京集奥聚合科技有限公司 All Rights Reserved.
 */
package com.betl.elastic.core.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;



public final class ElasticFactory {
	 static Client client=null;
	
	public static  Client getClient(){
		// 设置client.transport.sniff为true来使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中，
        // 这样做的好处是一般你不用手动设置集群里所有集群的ip到连接客户端，它会自动帮你添加，并且自动发现新加入集群的机器。
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name","escloud").put("client.transport.sniff", true).build();
         client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress("10.111.32.90", 9300))
        .addTransportAddress(new InetSocketTransportAddress("10.111.32.91", 9300))
        .addTransportAddress(new InetSocketTransportAddress("10.111.32.92", 9300))
        .addTransportAddress(new InetSocketTransportAddress("10.111.32.93", 9300))
        .addTransportAddress(new InetSocketTransportAddress("10.111.32.94", 9300));
        return client;
    }
	
	public static void close(){
		if(client!=null){
			client.close();
		}
	}
}