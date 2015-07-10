package no.vimond.StorageArchitecture.Consumer;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import no.vimond.StorageArchitecture.Utils.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class FileSystemWriter extends TimerTask
{

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemWriter.class);

	private EventsKafkaConsumer consumer;
	private long currentTime;
	private ObjectMapper mapper;
	private StringBuffer stringBuffer;

	public FileSystemWriter(EventsKafkaConsumer consumer)
	{
		this.consumer = consumer;
		this.mapper = ObjectMapperConfiguration.configure();
		this.stringBuffer = new StringBuffer();
	}

	@Override
	public void run()
	{
		try
		{
			LOG.info("Going to flush the buffer into file now!");
			this.consumer.copyMessagesOnFlushArray();
			List<VimondEventAny> messages = this.consumer.getMessages();
			this.currentTime = System.currentTimeMillis();
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Constants.MESSAGE_PATH + Constants.MESSAGE_FILE_NAME + currentTime + ".txt"), "UTF-8"));

			for (VimondEventAny message : messages)
			{
				String JSONMessage;
				try
				{
					JSONMessage = mapper.writeValueAsString(message);
				} catch (JsonProcessingException e)
				{
					JSONMessage = null;
				}

				if (JSONMessage != null)
				{
					this.stringBuffer.append(JSONMessage + "\n");
				}
			}

			//Write only if there are messages
			if (this.stringBuffer.length() > 0)
			{
				bw.write(this.stringBuffer.toString());
				bw.flush();
				bw.close();
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
