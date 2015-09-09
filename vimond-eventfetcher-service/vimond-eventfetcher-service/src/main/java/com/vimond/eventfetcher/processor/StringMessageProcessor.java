package com.vimond.eventfetcher.processor;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import com.ecyrd.speed4j.StopWatch;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.eventfetcher.util.Constants;

/**
 * Simple string processor. Decodes a byte array into a String and puts it into the buffer 
 * @author matteoremoluzzi
 *
 */
public class StringMessageProcessor extends BatchProcessor implements MessageProcessor<String>
{
	private Logger LOG = LogManager.getLogger(StringMessageProcessor.class);
	private static final Marker messages = MarkerManager.getMarker("PERFORMANCES-THOUGHPUT");
	private static final Marker errors = MarkerManager.getMarker("PERFORMANCES-ERROR");
	private static final double FROM_NANOS_TO_MILLIS = 0.0000001;
	
	private int processedMessages;
	private StopWatch throughput;
	
	private final Decoder<String> stringDecoder = new StringDecoder();

	public Decoder<String> getDecoderSingleton()
	{
		return this.stringDecoder;
	}
	
	@SuppressWarnings("unchecked")
	public boolean process(String message, int arg1)
	{
		LOG.info(messages, toString() + " received message");
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
		return "HDFSMessageProcessor: ";
	}

}
