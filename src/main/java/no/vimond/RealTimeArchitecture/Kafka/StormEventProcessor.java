package no.vimond.RealTimeArchitecture.Kafka;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vimond.common.kafka07.consumer.JsonDecoder;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.common.shared.ObjectMapperConfiguration;
import com.vimond.firehose.api.events.views.UserViewProgressEvent;

public class StormEventProcessor implements MessageProcessor<String>
{
	private static final Logger LOG = LoggerFactory
			.getLogger(StormEventProcessor.class);

	private StringDecoder decoder = new StringDecoder();
	private BlockingQueue<String> queue;

	public StormEventProcessor()
	{
		this.queue = new LinkedBlockingDeque<String>();
	}

	public Decoder<String> getDecoderSingleton()
	{
		return decoder;
	}

	public boolean process(String message, int retryCount)
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
	
	public BlockingQueue<String> getQueue()
	{
		return queue;
	}

}
