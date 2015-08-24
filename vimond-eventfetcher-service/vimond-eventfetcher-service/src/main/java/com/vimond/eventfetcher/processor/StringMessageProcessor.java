package com.vimond.eventfetcher.processor;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.vimond.common.kafka07.consumer.MessageProcessor;

/**
 * Simple string processor. Decodes a byte array into a String and puts it into the buffer 
 * @author matteoremoluzzi
 *
 */
public class StringMessageProcessor extends BatchProcessor implements MessageProcessor<String>
{
	private Logger LOG = LogManager.getLogger(StringMessageProcessor.class);
	private static final Marker messages = MarkerManager.getMarker("PERFORMANCES-EVENTFETCHER-MESSAGE");
	private static final Marker errors = MarkerManager.getMarker("MONITORING-EVENTFETCHER-ERROR");
	
	private final Decoder<String> stringDecoder = new StringDecoder();

	public Decoder<String> getDecoderSingleton()
	{
		return this.stringDecoder;
	}
	
	public boolean process(String message, int arg1)
	{
		LOG.info(messages, toString() + " received message");
		try
		{
			consumer.putMessageIntoBuffer(message);
			return true;
		}
		catch(Exception e)
		{
			LOG.error(errors, toString() + " error while processing the message: {}", e.getMessage());
			return false;
		}
	}
	
	public String toString()
	{
		return "HDFSMessageProcessor: ";
	}

}
