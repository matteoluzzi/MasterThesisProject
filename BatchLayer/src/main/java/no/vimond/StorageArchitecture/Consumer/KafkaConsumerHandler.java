package no.vimond.StorageArchitecture.Consumer;

import java.util.HashSet;
import java.util.Set;

import no.vimond.StorageArchitecture.App;
import no.vimond.StorageArchitecture.Processor.FileSystemMessageProcessor;
import no.vimond.StorageArchitecture.Processor.HDFSMessageProcessor;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Utils.KafkaProperties;

import org.apache.hadoop.hdfs.tools.HDFSConcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.pail.Pail;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;

public class KafkaConsumerHandler {
	
	private final Logger LOG = LoggerFactory.getLogger(KafkaConsumerHandler.class);

	private KafkaProperties properties;
	private Set<EventsKafkaConsumer> consumerGroups;
	private KafkaConfig kafkaConfig;
	
	public KafkaConsumerHandler()
	{
		this.properties = new KafkaProperties();
		this.consumerGroups  = new HashSet<EventsKafkaConsumer>();
		this.initializeKafkaConfig();
		this.initializeKafkaConsumerConfig();
	}

	public void registerConsumerGroup()
	{
		KafkaConsumerConfig kafkaConsumerConfig = this.initializeKafkaConsumerConfig();
		
		EventsKafkaConsumer group = new EventsKafkaConsumer(new MetricRegistry(), new HealthCheckRegistry(), this.kafkaConfig, kafkaConsumerConfig, new HDFSMessageProcessor());
		this.consumerGroups.add(group);
		LOG.info("KafkaConsumerHandler: added new group");	
	}
	
	public void startListening()
	{
		
		for(EventsKafkaConsumer group : this.consumerGroups)
			try
			{
				group.start();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
	}
	
	private KafkaConsumerConfig initializeKafkaConsumerConfig()
	{
		KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig();
		kafkaConsumerConfig.consumerThreads = 20;
		kafkaConsumerConfig.groupid = Constants.DEFAULT_CONSUMER_GROUP_B;
		kafkaConsumerConfig.topic = Constants.DEFAULT_TOPIC;
		
		return kafkaConsumerConfig;
	}
	
	private void initializeKafkaConfig()
	{
		this.kafkaConfig = new KafkaConfig();
		this.kafkaConfig.hosts = (String) this.properties.getOrDefault("zookeeper.connect", Constants.DEFAULT_ZK_LOCATION);
		this.kafkaConfig.properties.put("autocommit.enable", "false");
	}
}
