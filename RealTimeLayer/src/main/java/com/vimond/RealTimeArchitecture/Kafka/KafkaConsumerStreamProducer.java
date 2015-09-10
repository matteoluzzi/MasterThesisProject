package com.vimond.RealTimeArchitecture.Kafka;

import com.vimond.utils.data.StormEvent;


/**
 * Interface for a stream producer
 * @author matteoremoluzzi
 *
 */
public interface KafkaConsumerStreamProducer
{
	public void startConsuming();
	
	public void shutdown();
	
	public StormEvent take();
	
	public StormEvent takeNoBlock();
	
}
