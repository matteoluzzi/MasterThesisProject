package com.vimond.eventfetcher.writer;

import java.io.IOException;
import java.util.List;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.vimond.pailStructure.TimeFramePailStructure;
import com.vimond.eventfetcher.consumer.KafkaConsumerEventFetcher;

/**
 * TimerTask used by the unreliable version of the kafka consumer for flushing
 * data into HDFS while keep reading from the broker
 * 
 * @author matteoremoluzzi
 *
 * @param <T>
 */
public class Writer<T> extends TimerTask
{
	private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

	private KafkaConsumerEventFetcher<T> consumer;

	public Writer(KafkaConsumerEventFetcher<T> consumer)
	{
		this.consumer = consumer;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void run()
	{
		LOG.info("Going to flush the buffer into file now!");

		List<Object> messages = this.consumer.getMessages();
		String location = null;

		location = this.consumer.getHDFSPathToLocation();

		if (location != null)
		{
			// Write only if there are messages
			if (messages.size() > 0)
			{
				Pail pail = null;
				try
				{
					pail = Pail.create(location, new TimeFramePailStructure());
				} catch (java.lang.IllegalArgumentException | IOException e)
				{
					try
					{
						pail = new Pail(location);
					} catch (IOException e1)
					{

					}
				}
				TypedRecordOutputStream os;
				try
				{
					os = pail.openWrite();
					os.writeObjects(messages.toArray());
					os.close();
				} catch (IOException e)
				{

				}
				messages.clear();
			}
		}
	}
}
