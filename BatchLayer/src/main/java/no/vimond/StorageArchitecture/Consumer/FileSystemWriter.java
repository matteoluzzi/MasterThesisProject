package no.vimond.StorageArchitecture.Consumer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.backtype.hadoop.pail.SequenceFileFormat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class FileSystemWriter extends TimerTask
{

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemWriter.class);

	private EventsKafkaConsumer consumer;
	private StringBuffer stringBuffer;

	public FileSystemWriter(EventsKafkaConsumer consumer)
	{
		this.consumer = consumer;
		this.stringBuffer = new StringBuffer();
	}

	@Override
	public void run()
	{
		try
		{
			LOG.info("Going to flush the buffer into file now!");
			this.consumer.copyMessagesOnFlushArray();
			List<Object> messages = this.consumer.getMessages();

			Pail<String> pail = new Pail("/Users/matteoremoluzzi/myPailFolder");

		//	pail.consolidate();

			// BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new
			// FileOutputStream(Constants.MESSAGE_PATH +
			// Constants.MESSAGE_FILE_NAME + currentTime + ".txt"), "UTF-8"));

			for (Object message : messages)
			{
				this.stringBuffer.append(message + "\n");
			}

			// Write only if there are messages
			if (this.stringBuffer.length() > 0)
			{
				TypedRecordOutputStream os = pail.openWrite("test", false);
				os.writeObjects(this.stringBuffer.toString());

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
