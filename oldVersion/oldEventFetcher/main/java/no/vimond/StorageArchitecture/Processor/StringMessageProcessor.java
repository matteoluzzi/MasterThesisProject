package no.vimond.StorageArchitecture.Processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

import com.vimond.common.kafka07.consumer.MessageProcessor;

public class StringMessageProcessor extends BatchProcessor implements MessageProcessor<String>
{
	private Logger LOG = LoggerFactory.getLogger(StringMessageProcessor.class);
	
	private final Decoder<String> stringDecoder = new StringDecoder();

	public Decoder<String> getDecoderSingleton()
	{
		return this.stringDecoder;
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
