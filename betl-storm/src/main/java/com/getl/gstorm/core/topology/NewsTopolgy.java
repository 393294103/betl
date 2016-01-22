package com.getl.gstorm.core.topology;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.getl.gstorm.core.bolt.MsgScheme;
import com.getl.gstorm.core.bolt.MsgSplitBolt;

public class NewsTopolgy {

	public StormTopology createTopology() {
		// 创建kafka的spout
		String topic = "sinanews";
		String zkRoot = "/storm-kafka";
		String spoutId = "kafkaSpout";
		BrokerHosts brokerHosts = new ZkHosts("10.111.32.203:2181");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
		kafkaConfig.forceFromStart = true;
		// kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.scheme = new SchemeAsMultiScheme(new MsgScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 2);
		builder.setBolt("MsgSplitBolt", new MsgSplitBolt()).shuffleGrouping("KafkaSpout");

		// 创建一个hdfs的bolt
		// use "|" instead of "," for field delimiter
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\t");
		// sync the filesystem after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);
		// rotate files
		// FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f,
		// TimeUnit.MINUTES);
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/storm/raw/sinanews").withPrefix("sinanews_").withExtension(".txt");
		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://10.111.32.202:8020").withFileNameFormat(fileNameFormat).withRecordFormat(format)
		 .withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy);

		builder.setBolt("HdfsBolt", hdfsBolt, 1).fieldsGrouping("MsgSplitBolt", new Fields("url", "title", "content"));
		return builder.createTopology();
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		System.setProperty("hadoop.home.dir", "D:\\work_soft\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		conf.setMaxTaskParallelism(2);
		// conf.put(key, value);

		conf.setNumWorkers(3);
		if (args.length > 0) {
			conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, args[0]);
			StormSubmitter.submitTopology("NewsTopolgy", conf, new NewsTopolgy().createTopology());
			// StormSubmitter.submitTopologyWithProgressBar("NewsTopolgy", conf,
			// new NewsTopolgy().createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("NewsTopolgy", conf, new NewsTopolgy().createTopology());
		}

	}
}
