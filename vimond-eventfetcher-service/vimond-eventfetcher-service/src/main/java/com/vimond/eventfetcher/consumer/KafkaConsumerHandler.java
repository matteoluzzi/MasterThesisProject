package com.vimond.eventfetcher.consumer;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.messages.MessageConsumer;
import com.vimond.eventfetcher.configuration.KafkaConsumerEventFetcherConfiguration;
import com.vimond.eventfetcher.configuration.ProcessorConfiguration;
import com.vimond.eventfetcher.configuration.VimondEventFetcherServiceConfiguration;

import com.vimond.eventfetcher.processor.BatchProcessor;
import com.vimond.eventfetcher.processor.BatchProcessorEnum;
import com.vimond.eventfetcher.processor.BatchProcessorFactory;
import com.vimond.eventfetcher.util.Constants;

/**
 * High level abstraction over a kafka consumer. Responsible for create consumers and subscribe them to a topic
 * @author matteoremoluzzi
 *
 */
public class KafkaConsumerHandler<T> {
	
	private final Logger LOG = LoggerFactory.getLogger(KafkaConsumerHandler.class);

	private KafkaConsumerEventFetcherConfiguration consumerConfig;
	private Set<MessageConsumer<T>> consumerGroups;
	private KafkaConfig kafkaConfig;
	private ProcessorConfiguration processorConfig;
	
	public KafkaConsumerHandler(VimondEventFetcherServiceConfiguration configuration)
	{
		this.consumerConfig = configuration.getKafkaConsumerConfig();
		this.processorConfig = configuration.getProcessorConfiguration();
		this.consumerGroups  = new HashSet<MessageConsumer<T>>();
		this.kafkaConfig = configuration.getKafkaConfig();
	}

	public void registerConsumerGroup()
	{
		initializeKafkaConfig();
		initializeKafkaConsumerConfig();
		BatchProcessor processor = BatchProcessorFactory.getFactory().createMessageProcessor(BatchProcessorEnum.STRING);
		MessageConsumer<T> group = KafkaConsumerFactory.getFactory().createMessageProcessor(KafkaConsumerEnum.valueOf(consumerConfig.getMode()), new MetricRegistry(), new HealthCheckRegistry(), this.kafkaConfig, this.consumerConfig, processor, this.processorConfig);
		this.consumerGroups.add(group);
		LOG.info("KafkaConsumerHandler: added new group");	
	}
	
	public void startListening()
	{
		for(MessageConsumer<T> group : this.consumerGroups)
			try
			{
				group.start();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
	}
	
	/**
	 * check if the basic properties has been set by yaml file
	 */
	private void initializeKafkaConsumerConfig()
	{
		if(consumerConfig.consumerThreads == 0)
			consumerConfig.consumerThreads = Constants.DEFAULT_CONSUMER_THREAD;
		
		if(consumerConfig.groupid == null)
			consumerConfig.groupid = Constants.DEFAULT_CONSUMER_GROUP_B;
		
		if(consumerConfig.topic == null)
			consumerConfig.topic = Constants.DEFAULT_TOPIC;
	}
	
	/**
	 * check if the basic properties has been set by yaml file
	 */
	private void initializeKafkaConfig()
	{
		if(kafkaConfig.hosts == null)
			kafkaConfig.hosts = Constants.DEFAULT_ZK_LOCATION;
	}
	
	public void shutdown()
	{
		for(MessageConsumer<T> group : this.consumerGroups)
			try
			{
				group.stop();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
	}
}
