package no.vimond.RealTimeArchitecture.Spout;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import no.vimond.RealTimeArchitecture.Kafka.StormEventSchema;
import no.vimond.RealTimeArchitecture.Utils.AppProperties;
import no.vimond.RealTimeArchitecture.Utils.Constants;
import no.vimond.RealTimeArchitecture.Utils.Utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.KafkaConfig.BrokerHosts;
import storm.kafka.KafkaConfig.ZkHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;

public class SpoutCreator
{
	private static Logger LOG = LoggerFactory.getLogger(SpoutCreator.class);

	public static IRichSpout create(AppProperties props)
	{
		KafkaAPI api_version = (props.get("kafka_api_version") != null) ? KafkaAPI.valueOf((String) props.get("kafka_api_version")) : KafkaAPI.DEFAULT;

		switch (api_version)
		{
		case API07:
			return new KafkaSpout07();
		case API08:
			return buildKafka08Spout(props);
		case API07KS:
			return createKafkaSpout07(props);
		default:
		{
			LOG.warn("Usage of default API (07)");
			return createKafkaSpout07(props);
		}
		}
	}

	private static KafkaSpout buildKafka08Spout(AppProperties props)
	{
		SpoutConfig cfg = buildKakfaConfig(props);
		return new KafkaSpout(cfg);
	}

	private static SpoutConfig buildKakfaConfig(Properties props)
	{
		String zKLocation = (props.getProperty("zookeeper.connect") != null) ? props.getProperty("zookeeper.connect") : Constants.DEFAULT_ZK_LOCATION;
		long zKPort = (Long) ((props.get("zookeeper.port") != null) ? props.get("zookeeper.port") : Constants.DEFAULT_ZK_PORT);
		String topic = (props.getProperty("topic") != null) ? props.getProperty("topic") : Constants.DEFAULT_TOPIC;
		String consumer_group = (props.getProperty("group.id") != null) ? props.getProperty("group.id") : Constants.DEFAULT_CONSUMER_GROUP_RT;

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

		BrokerHosts zkHost = new ZkHosts(zKLocation + ":" + zKPort, "/");

		SpoutConfig cfg = new SpoutConfig(zkHost, topic, "/" + topic, consumer_group);

		// start reading from the end of the topic --> valid only for the first
		// run of the topology, then it starts according to the offset stored on
		// ZK
		// cfg.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

		return cfg;
	}

	public static KafkaSpoutStorm createKafkaSpout07(AppProperties props)
	{
		String zKLocation = (props.getProperty("zookeeper.connect") != null) ? props.getProperty("zookeeper.connect") : Constants.DEFAULT_ZK_LOCATION;
		long zKPort = (props.getProperty("zookeeper.port") != null) ? Long.parseLong(props.getProperty("zookeeper.port")) : Constants.DEFAULT_ZK_PORT;
		String zKBrokerPath = (props.getProperty("zookeeper.brokerPath") != null) ? props.getProperty("zookeeper.brokerPath") : Constants.DEFAULT_ZK_BROKER_PATH;
		String topic = (props.getProperty("topic") != null) ? props.getProperty("topic") : Constants.DEFAULT_TOPIC;
		String consumer_group = (props.getProperty("group.id") != null) ? props.getProperty("group.id") : Constants.DEFAULT_CONSUMER_GROUP_RT;

		BrokerHosts z = new ZkHosts(zKLocation + ":" + zKPort, zKBrokerPath);
		SpoutConfig cfg = new SpoutConfig(z, topic, "/" + topic, consumer_group);
		cfg.scheme = new SchemeAsMultiScheme(new StormEventSchema());

		KafkaSpoutStorm spout = new KafkaSpoutStorm(cfg);
		return spout;

	}
}
