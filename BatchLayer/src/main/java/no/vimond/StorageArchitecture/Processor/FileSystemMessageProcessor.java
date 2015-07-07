package no.vimond.StorageArchitecture.Processor;

import kafka.serializer.Decoder;
import no.vimond.StorageArchitecture.Consumer.EventsKafkaConsumer;
import no.vimond.StorageArchitecture.Utils.StormEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.kafka07.consumer.JsonDecoder;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class FileSystemMessageProcessor implements MessageProcessor<VimondEventAny>
{
	
	private Logger LOG = LoggerFactory.getLogger(FileSystemMessageProcessor.class);

	private JsonDecoder<VimondEventAny> jsonDecoder;
	private EventsKafkaConsumer consumer;
	
	
	public FileSystemMessageProcessor()
	{
		this.jsonDecoder = new JsonDecoder<VimondEventAny>(StormEvent.class, ObjectMapperConfiguration.configurePretty());
	}
	
	public void setEventsKafkaConsumer(EventsKafkaConsumer consumer)
	{
		this.consumer = consumer;
	}
	
	public Decoder<VimondEventAny> getDecoderSingleton()
	{
		return jsonDecoder;
	}

	public boolean process(VimondEventAny message, int arg1)
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
		return "FileSystemMessageProcessor: ";
	}
}
