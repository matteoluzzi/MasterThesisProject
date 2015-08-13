package com.vimond.eventfetcher;

import io.dropwizard.Configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vimond.common.kafka07.KafkaConfig;

/**
 * Client for this service.
 *
 * @author Matteo Remo Luzzi	 
 * @mailto:matteo@vimond.com
 * @since 2015-08-07
 */
public class VimondEventFetcherServiceConfiguration extends Configuration {

	@JsonProperty("kafka")
	private KafkaConfig kafkaConfig;
	
	@JsonProperty("consumer")
	private KafkaConsumerConfigEventFetcher kafkaConsumerConfig;
	
	@JsonProperty("processor")
    private ProcessorConfiguration processor;
	
	public KafkaConfig getKafkaConfig()
	{
		return kafkaConfig;
	}

	public KafkaConsumerConfigEventFetcher getKafkaConsumerConfig()
	{
		return kafkaConsumerConfig;
	}

	public ProcessorConfiguration getProcessorConfiguration()
	{
		return processor;
	}

	public void setKafkaConfig(KafkaConfig kafkaConfig)
	{
		this.kafkaConfig = kafkaConfig;
	}

	public void setKafkaConsumerConfig(KafkaConsumerConfigEventFetcher kafkaConsumerConfig)
	{
		this.kafkaConsumerConfig = kafkaConsumerConfig;
	}

	public void setProcessorConfiguration(ProcessorConfiguration processor)
	{
		this.processor = processor;
	}
	
	

}
