package com.vimond.eventfetcher.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;

public class KafkaConsumerEventFetcherConfiguration extends KafkaConsumerConfig
{

	@JsonProperty
	private String mode;

	public String getMode()
	{
		return mode;
	}

	public void setMode(String mode)
	{
		this.mode = mode;
	}
	
}
