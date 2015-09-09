package no.vimond.StorageArchitecture.Processor;

import no.vimond.StorageArchitecture.Consumer.EventsKafkaConsumer;

public abstract class BatchProcessor
{
	protected EventsKafkaConsumer consumer;
	
	public void setEventsKafkaConsumer(EventsKafkaConsumer consumer)
	{
		this.consumer = consumer;
	}
}
