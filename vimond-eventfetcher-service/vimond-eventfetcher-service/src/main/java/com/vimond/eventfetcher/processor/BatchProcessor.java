package com.vimond.eventfetcher.processor;

import com.vimond.eventfetcher.consumer.KafkaConsumerEventFetcher;
/**
 * Abstract class for a message processor. Needed for setting the Consumer instance into it
 * @author matteoremoluzzi
 *
 */
public abstract class BatchProcessor
{
	protected KafkaConsumerEventFetcher consumer;
	
	public void setEventsKafkaConsumer(KafkaConsumerEventFetcher consumer)
	{
		this.consumer = consumer;
	}
}
