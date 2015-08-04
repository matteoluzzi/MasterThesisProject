package no.vimond.RealTimeArchitecture.Kafka;

import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.serializer.Decoder;
import no.vimond.RealTimeArchitecture.Utils.StormEvent;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vimond.common.kafka07.consumer.JsonDecoder;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class StormEventProcessor implements MessageProcessor<StormEvent>
{
	private static final Logger LOG = LoggerFactory
			.getLogger(StormEventProcessor.class);

	private JsonDecoder<StormEvent> jsonDecoder;
	private ConcurrentLinkedQueue<StormEvent> queue;

	public StormEventProcessor()
	{
		this.jsonDecoder = new JsonDecoder<StormEvent>(StormEvent.class, ObjectMapperConfiguration.configurePretty());
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
				message.setInitTime(new DateTime().getMillis());
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
