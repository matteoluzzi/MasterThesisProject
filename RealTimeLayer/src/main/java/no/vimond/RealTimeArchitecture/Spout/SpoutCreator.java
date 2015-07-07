package no.vimond.RealTimeArchitecture.Spout;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import no.vimond.RealTimeArchitecture.Utils.Constants;
import no.vimond.RealTimeArchitecture.Utils.Utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.topology.IRichSpout;

public class SpoutCreator
{
	private static Logger LOG = LoggerFactory.getLogger(SpoutCreator.class);

	public static IRichSpout create(Map<String, String> args)
	{
		KafkaAPI api_version = (args.get("kafka_api_version") != null) ? KafkaAPI
				.valueOf(args.get("kafka_api_version")) : KafkaAPI.DEFAULT;

		switch (api_version)
		{
			case API07:
				return new KafkaSpout07();
			case API08:
				return buildKafka08Spout(args);
			default:
			{
				LOG.warn("Usage of default API (07)");
				return new KafkaSpout07();
			}
		}
	}

	private static KafkaSpout buildKafka08Spout(Map<String, String> args)
	{
		SpoutConfig cfg = buildKakfaConfig(args);
		return new KafkaSpout(cfg);
	}

	private static SpoutConfig buildKakfaConfig(Map<String, String> args)
	{
		String arg_zkHostAddress = (args.get("zkHost") != null) ? args.get("zkHost") : Constants.DEFAULT_ZK_LOCATION;
		String arg_topic = (args.get("topic") != null) ? args.get("topic") : Constants.DEFAULT_TOPIC;
		String arg_consumer_id = (args.get("consumer_group") != null) ? args.get("consumer_group") : Constants.DEFAULT_CONSUMER_GROUP_RT;
		
		InputStream propertyFileAsAInputStream = Utility.loadPropertiesFileFromClassPath("app.properties");
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
				prop.getProperty("zkHost"), "/") : new ZkHosts(arg_zkHostAddress, "/");

		
		SpoutConfig cfg = new SpoutConfig(zkHost, topic, "/" + topic ,arg_consumer_id);

		//start reading from the end of the topic --> valid only for the first run of the topology, then it starts according to the offset stored on ZK
	//	cfg.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		
		return cfg;
	}
}