package no.vimond.RealTimeArchitecture.Spout;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.vimond.RealTimeArchitecture.Utils.Utility;

import backtype.storm.topology.base.BaseRichSpout;

public class SpoutCreator
{
	private static Logger LOG = LoggerFactory.getLogger(SpoutCreator.class);

	public static BaseRichSpout create(Map<String, String> args)
	{
		KafkaAPI api_version = (args.get("kafka_api_version") != null) ? KafkaAPI
				.valueOf(args.get("kafka_api_version")) : KafkaAPI.DEFAULT;

		switch (api_version)
		{
			case API07:
				return new KafkaSpout07();
			case API08:
//				return buildKafka08Spout(zkAddr, topic);
			default:
			{
				LOG.warn("Usage of default API (07)");
				return new KafkaSpout07();
			}
		}
	}
/*
	private static KafkaSpout buildKafka08Spout(String zk, String topic)
	{
		SpoutConfig cfg = buildKakfaConfig(zk, topic);
		
		return  new KafkaSpout(cfg);

	}

	private static SpoutConfig buildKakfaConfig(String arg_zkHostAddress,
			String arg_topic)
	{
		InputStream propertyFileAsAInputStream = Utility
				.loadPropertiesFileFromClassPath("app.properties");
		Properties prop = new Properties();
		try
		{
			prop.load(propertyFileAsAInputStream);
		} catch (IOException e)
		{
			LOG.warn("Cannot load properties file, exiting now");
			System.exit(0);
		}
		
		String topic = (arg_topic == null) ? prop.getProperty("topic")
				: arg_topic;
		BrokerHosts zkHost = (arg_zkHostAddress == null) ? new ZkHosts(
				prop.getProperty("zkHost")) : new ZkHosts(arg_zkHostAddress);

		SpoutConfig cfg = new SpoutConfig(zkHost, topic, "/" + topic, UUID
				.randomUUID().toString());

		//start reading from the end of the topic --> valid only for the first run of the topology, then it starts according to the offset stored on ZK
		cfg.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		
		return cfg;
	}*/
}
