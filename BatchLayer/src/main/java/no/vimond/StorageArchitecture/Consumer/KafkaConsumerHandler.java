package no.vimond.StorageArchitecture.Consumer;

import java.util.HashSet;
import java.util.Set;

import no.vimond.StorageArchitecture.Processor.StringMessageProcessor;
import no.vimond.StorageArchitecture.Utils.AppProperties;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Utils.KafkaProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;

/**
 * High level abstraction over a kafka consumer. Responsible for create cosumers and subscribe them to a topic
 * @author matteoremoluzzi
 *
 */
public class KafkaConsumerHandler {
	
	private final Logger LOG = LoggerFactory.getLogger(KafkaConsumerHandler.class);

	private KafkaProperties properties;
	private Set<EventsKafkaConsumer> consumerGroups;
	private KafkaConfig kafkaConfig;
	private AppProperties appProps;
	
	public KafkaConsumerHandler(AppProperties appProps)
	{
		this.properties = new KafkaProperties();
		this.consumerGroups  = new HashSet<EventsKafkaConsumer>();
		this.appProps = appProps;
		this.initializeKafkaConfig();
		this.initializeKafkaConsumerConfig();
	}

	public void registerConsumerGroup()
	{
		KafkaConsumerConfig kafkaConsumerConfig = this.initializeKafkaConsumerConfig();
		
		EventsKafkaConsumer group = new EventsKafkaConsumer(new MetricRegistry(), new HealthCheckRegistry(), this.kafkaConfig, kafkaConsumerConfig, new StringMessageProcessor(), this.appProps);
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
		kafkaConsumerConfig.consumerThreads = (Integer) this.appProps.getOrDefault(Constants.CONSUMER_THREAD_KEY, Constants.DEFAULT_CONSUMER_THREAD);
		kafkaConsumerConfig.groupid = (String) this.appProps.getOrDefault(Constants.CONSUMER_GROUP_KEY, Constants.DEFAULT_CONSUMER_GROUP_B);
		kafkaConsumerConfig.topic = (String) this.appProps.getOrDefault(Constants.TOPIC_KEY, Constants.DEFAULT_TOPIC);;
		
		return kafkaConsumerConfig;
	}
	
	private void initializeKafkaConfig()
	{
		this.kafkaConfig = new KafkaConfig();
		this.kafkaConfig.hosts = (String) this.properties.getOrDefault(this.appProps.get(Constants.ZK_KEY), Constants.DEFAULT_ZK_LOCATION);
		this.kafkaConfig.properties.put("autocommit.enable", "false");
	}
	
	public void shutdown()
	{
		for(EventsKafkaConsumer group : this.consumerGroups)
			try
			{
				group.shutdown();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
	}
}
