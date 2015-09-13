package com.vimond.eventfetcher.processor;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vimond.common.kafka07.consumer.MessageProcessor;

/**
 * Simple string processor. Decodes a byte array into a String and puts it into the buffer 
 * @author matteoremoluzzi
 *
 */
public class StringMessageProcessor extends BatchProcessor implements MessageProcessor<String>
{
	private final Logger LOG = LoggerFactory.getLogger(getClass());
	
	private final Decoder<String> stringDecoder = new StringDecoder();

	public Decoder<String> getDecoderSingleton()
	{
		return this.stringDecoder;
	}
	
	@SuppressWarnings("unchecked")
	public boolean process(String message, int arg1)
	{
		LOG.debug(toString() + " received message");
		try
		{
			consumer.putMessageIntoBuffer(message);
			return true;
		}
		catch(Exception e)
		{
			LOG.error(toString() + " error while processing the message: {}", e.getMessage());
			return false;
		}
	}
	
	public String toString()
	{
		return "HDFSMessageProcessor: ";
	}

}
