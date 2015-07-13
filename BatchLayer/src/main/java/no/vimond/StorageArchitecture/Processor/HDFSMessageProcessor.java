package no.vimond.StorageArchitecture.Processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.vimond.StorageArchitecture.Consumer.EventsKafkaConsumer;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

import com.vimond.common.kafka07.consumer.MessageProcessor;

public class HDFSMessageProcessor implements MessageProcessor<String>
{
	private Logger LOG = LoggerFactory.getLogger(HDFSMessageProcessor.class);
	
	private final Decoder<String> stringDecoder = new StringDecoder();
	private EventsKafkaConsumer consumer;

	public Decoder<String> getDecoderSingleton()
	{
		return this.stringDecoder;
	}
	
	public void setEventsKafkaConsumer(EventsKafkaConsumer consumer)
	{
		this.consumer = consumer;
	}

	public boolean process(String message, int arg1)
	{
		LOG.info(toString() + " received message");
		try
		{
			consumer.putMessageIntoBuffer(message);
			return true;
		}
		catch(Exception e)
		{
			LOG.info(toString() + " error while processing the message ");
			e.printStackTrace();
			return false;
		}
	}
	
	public String toString()
	{
		return "HDFSMessageProcessor: ";
	}

}
