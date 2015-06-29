package no.vimond.RealTimeArchitecture.Kafka;

import no.vimond.RealTimeArchitecture.Utils.StormEvent;


public interface KafkaConsumerStreamProducer
{
	public void startConsuming();
	
	public void shutdown();
	
	public StormEvent take();
	
	public StormEvent takeNoBlock();
	
}
