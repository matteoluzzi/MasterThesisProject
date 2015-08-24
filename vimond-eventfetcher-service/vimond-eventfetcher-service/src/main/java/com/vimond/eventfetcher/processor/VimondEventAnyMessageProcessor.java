package com.vimond.eventfetcher.processor;

import kafka.serializer.Decoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.kafka07.consumer.JsonDecoder;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.common.shared.ObjectMapperConfiguration;

/**
 * VimondEventAny processor. It decodes a VimondEventAny object from a byte[] and puts it into the buffer
 * @author matteoremoluzzi
 *
 */
public class VimondEventAnyMessageProcessor extends BatchProcessor implements MessageProcessor<VimondEventAny>
{
	
	private Logger LOG = LogManager.getLogger(VimondEventAnyMessageProcessor.class);
	private static final Marker messages = MarkerManager.getMarker("PERFORMANCES-MESSAGE");
	private static final Marker errors = MarkerManager.getMarker("PERFORMANCES-ERROR");

	private JsonDecoder<VimondEventAny> jsonDecoder;
	
	public VimondEventAnyMessageProcessor()
	{
		this.jsonDecoder = new JsonDecoder<VimondEventAny>(VimondEventAny.class, ObjectMapperConfiguration.configurePretty());
	}
	
	public Decoder<VimondEventAny> getDecoderSingleton()
	{
		return jsonDecoder;
	}

	public boolean process(VimondEventAny message, int arg1)
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
		return "FileSystemMessageProcessor: ";
	}
}
