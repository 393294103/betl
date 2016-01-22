/**
 * @Email:zhanghelin@geotmt.com
 * @Author:Zhl
 * @Date:2015��12��23������5:24:42
 * @Desc:
 * @Copyright (c) 2014, �������¾ۺϿƼ����޹�˾ All Rights Reserved.
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
		// ����client.transport.sniffΪtrue��ʹ�ͻ���ȥ��̽������Ⱥ��״̬���Ѽ�Ⱥ������������ip��ַ�ӵ��ͻ����У�
        // �������ĺô���һ���㲻���ֶ����ü�Ⱥ�����м�Ⱥ��ip�����ӿͻ��ˣ������Զ�������ӣ������Զ������¼��뼯Ⱥ�Ļ�����
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