package com.vimond.RealTimeArchitecture.Kafka;

import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.serializer.Decoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.common.kafka07.consumer.JsonDecoder;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.utils.data.StormEvent;

/**
 * Event processor for custom kafka spout implementation. It reads a message, deserialize it and put it in a queue for topology ingestion
 * @author matteoremoluzzi
 *
 */
public class StormEventProcessor implements MessageProcessor<StormEvent>
{
	private static final Logger LOG = LoggerFactory
			.getLogger(StormEventProcessor.class);

	private JsonDecoder<StormEvent> jsonDecoder;
	private ConcurrentLinkedQueue<StormEvent> queue;

	public StormEventProcessor()
	{
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new JodaModule());
		this.jsonDecoder= new JsonDecoder<StormEvent>(StormEvent.class, mapper);
		this.queue = new ConcurrentLinkedQueue<StormEvent>();
	}

	public Decoder<StormEvent> getDecoderSingleton()
	{
		return jsonDecoder;
	}

	public boolean process(StormEvent message, int retryCount)
	{
		if (message != null)
			try
			{
				queue.add(message);
				return true;
			} catch (IllegalStateException e)
			{
				LOG.error(null, e);
			} catch (Exception e)
			{
				LOG.error(null, e);
			}
		return false;
	}
	
	public ConcurrentLinkedQueue<StormEvent> getQueue()
	{
		return queue;
	}
}
