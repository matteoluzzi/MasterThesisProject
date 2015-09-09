package no.vimond.StorageArchitecture.Writer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.TimerTask;

import no.vimond.StorageArchitecture.Consumer.EventsKafkaConsumer;
import no.vimond.StorageArchitecture.Utils.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

public class HDFSWriter extends TimerTask
{

	private static final Logger LOG = LoggerFactory.getLogger(HDFSWriter.class);

	private EventsKafkaConsumer consumer;

	public HDFSWriter(EventsKafkaConsumer consumer)
	{
		this.consumer = consumer;
	}

	@SuppressWarnings({ "rawtypes", "unchecked"})
	@Override
	public void run()
	{
		try
		{
			LOG.info("Going to flush the buffer into file now!");
			
			this.consumer.copyMessagesOnFlushArray();
			List<Object> messages = this.consumer.getMessages();
		
			// Write only if there are messages
			if (messages.size() > 0)
			{
				Pail pail = new Pail(Constants.NEW_DATA_PATH);
				TypedRecordOutputStream os = pail.openWrite();
				os.writeObjects(messages.toArray());
				os.close();
			}
			this.consumer.getMessages().clear();

		} catch (UnsupportedEncodingException e1)
		{
			e1.printStackTrace();
		} catch (FileNotFoundException e1)
		{
			e1.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

}
