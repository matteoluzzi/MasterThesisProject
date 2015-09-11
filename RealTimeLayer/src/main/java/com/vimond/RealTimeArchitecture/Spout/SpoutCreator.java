package com.vimond.RealTimeArchitecture.Spout;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.storm.zookeeper.client.ConnectStringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;
import storm.kafka.KafkaConfig.BrokerHosts;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaConfig.ZkHosts;
import storm.kafka.SpoutConfig;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;

import com.vimond.RealTimeArchitecture.Kafka.StormEventSchema;
import com.vimond.RealTimeArchitecture.Kafka.kafkalibrary.KafkaSpout;
import com.vimond.RealTimeArchitecture.Utils.Utility;
import com.vimond.utils.config.AppProperties;
import com.vimond.utils.data.Constants;
/**
 * Class which acts as a factory for kafkaspout creation. It creates a different object according to the value specified in props file
 * @author matteoremoluzzi
 *
 */
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

	//Not tested
	private static KafkaSpout buildKafka08Spout(AppProperties props)
	{
		SpoutConfig cfg = buildKakfaConfig(props);
		return new KafkaSpout(cfg);
	}

	
	//Not tested
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

		return cfg;
	}

	/**
	 * Create a Kafka spout using the Kafka-Storm API
	 * @param props
	 * @return
	 */
	public static KafkaSpout createKafkaSpout07(AppProperties props)
	{
		
		final String zKLocation = (props.getProperty("zookeeper.connect") != null) ? props.getProperty("zookeeper.connect") : Constants.DEFAULT_ZK_LOCATION;
//		final long zKPort = (props.getProperty("zookeeper.port") != null) ? Long.parseLong(props.getProperty("zookeeper.port")) : Constants.DEFAULT_ZK_PORT;
//		final String zKBrokerPath = (props.getProperty("zookeeper.brokerPath") != null) ? props.getProperty("zookeeper.brokerPath") : Constants.DEFAULT_ZK_BROKER_PATH;
		final String topic = (props.getProperty("topic") != null) ? props.getProperty("topic") : Constants.DEFAULT_TOPIC;
		final String consumer_group = (props.getProperty("group.id") != null) ? props.getProperty("group.id") : Constants.DEFAULT_CONSUMER_GROUP_RT;
		final String forceFromStart = (props.getProperty("forceFromStart") != null) ? props.getProperty("forceFromStart") : "false";
		
		final ConnectStringParser connectStringParser = new ConnectStringParser(zKLocation);
		final List<InetSocketAddress> serverInetAddresses = connectStringParser.getServerAddresses();
		final List<String> serverAddresses = new ArrayList<String>(serverInetAddresses.size());
		final Integer zkPort_ = serverInetAddresses.get(0).getPort();
		for (InetSocketAddress serverInetAddress : serverInetAddresses) {
	        serverAddresses.add(serverInetAddress.getHostName());
	    }
		
		//Use a static configuration for connecting to EC2 broker cluster. Using dynamic host resolution through zookeeper seems not to work
		@SuppressWarnings("unchecked")
		BrokerHosts bhs = StaticHosts.fromHostString(Arrays.asList(new String[] {zKLocation}), 6);
		
		//final ZkHosts z = new ZkHosts(zKLocation + ":" + zKPort, zKBrokerPath);
		final SpoutConfig cfg = new SpoutConfig(bhs, topic, "/" + topic, consumer_group);
		
		cfg.zkServers = serverAddresses;
		cfg.zkPort = zkPort_;
		cfg.zkRoot = "/" + topic;
		
		cfg.scheme = new SchemeAsMultiScheme(new StormEventSchema());
		cfg.forceFromStart = Boolean.parseBoolean(forceFromStart);
		cfg.fetchSizeBytes = 1024*1024*4;
		cfg.bufferSizeBytes = 1024*1024*8;

		KafkaSpout spout = new KafkaSpout(cfg);
		return spout;

	}
}
