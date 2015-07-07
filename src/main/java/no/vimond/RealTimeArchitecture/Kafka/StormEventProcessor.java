package no.vimond.RealTimeArchitecture.Kafka;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import kafka.serializer.Decoder;
import no.vimond.RealTimeArchitecture.Utils.StormEvent;

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
	private BlockingQueue<StormEvent> queue;

	public StormEventProcessor()
	{
		this.jsonDecoder = new JsonDecoder<StormEvent>(StormEvent.class, ObjectMapperConfiguration.configurePretty());
		this.queue = new ArrayBlockingQueue<StormEvent>(2000);
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
	
	public BlockingQueue<StormEvent> getQueue()
	{
		return queue;
	}
}
