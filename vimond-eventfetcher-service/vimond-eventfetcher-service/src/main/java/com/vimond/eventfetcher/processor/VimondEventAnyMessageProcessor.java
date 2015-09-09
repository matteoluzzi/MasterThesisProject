package com.vimond.eventfetcher.processor;

import kafka.serializer.Decoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.ecyrd.speed4j.StopWatch;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.kafka07.consumer.JsonDecoder;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.common.shared.ObjectMapperConfiguration;
import com.vimond.eventfetcher.util.Constants;

/**
 * VimondEventAny processor. It decodes a VimondEventAny object from a byte[] and puts it into the buffer
 * @author matteoremoluzzi
 *
 */
public class VimondEventAnyMessageProcessor extends BatchProcessor implements MessageProcessor<VimondEventAny>
{
	
	private Logger LOG = LogManager.getLogger(VimondEventAnyMessageProcessor.class);
	private static final Marker messages = MarkerManager.getMarker("PERFORMANCES-THOUGHPUT");
	private static final Marker errors = MarkerManager.getMarker("PERFORMANCES-ERROR");
	private static final double FROM_NANOS_TO_MILLIS = 0.0000001;
	
	private int processedMessages;
	private StopWatch throughput;

	private JsonDecoder<VimondEventAny> jsonDecoder;
	
	public VimondEventAnyMessageProcessor()
	{
		this.jsonDecoder = new JsonDecoder<VimondEventAny>(VimondEventAny.class, ObjectMapperConfiguration.configurePretty());
		this.throughput = new StopWatch();
		this.throughput.start();
	}
	
	public Decoder<VimondEventAny> getDecoderSingleton()
	{
		return jsonDecoder;
	}

	@SuppressWarnings("unchecked")
	public boolean process(VimondEventAny message, int arg1)
	{
		
		try
		{
			consumer.putMessageIntoBuffer(message);
			if(++processedMessages == Constants.BATCH_STATISTICS)
			{
				this.throughput.stop();
				double avg_throughput = Constants.BATCH_STATISTICS / (this.throughput.getTimeNanos() * FROM_NANOS_TO_MILLIS);
				LOG.info(messages, avg_throughput);
				this.processedMessages = 0;
				this.throughput.start();
			}
			return true;
		}
		catch(Exception e)
		{
			LOG.error(errors, toString() + " error while processing the message: {}", e.getMessage());
			if(++processedMessages == Constants.BATCH_STATISTICS)
			{
				this.throughput.stop();
				double avg_throughput = Constants.BATCH_STATISTICS / (this.throughput.getTimeNanos() * FROM_NANOS_TO_MILLIS);
				LOG.info(messages, avg_throughput);
				this.processedMessages = 0;
				this.throughput.start();
			}
			return false;
		}
	}
	
	public String toString()
	{
		return "FileSystemMessageProcessor: ";
	}
}
