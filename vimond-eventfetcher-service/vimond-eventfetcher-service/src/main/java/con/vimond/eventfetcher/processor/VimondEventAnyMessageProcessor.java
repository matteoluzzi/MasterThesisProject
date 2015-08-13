package con.vimond.eventfetcher.processor;

import kafka.serializer.Decoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.kafka07.consumer.JsonDecoder;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.common.shared.ObjectMapperConfiguration;

/**
 * VimondEventAny processor. It decodes a VimondEventAny object from a byte[] and puts it into the buffer
 * @author matteoremoluzzi
 *
 */
public class VimondEventAnyMessageProcessor extends BatchProcessor implements MessageProcessor<VimondEventAny>
{
	
	private Logger LOG = LoggerFactory.getLogger(VimondEventAnyMessageProcessor.class);

	private JsonDecoder<VimondEventAny> jsonDecoder;
	
	public VimondEventAnyMessageProcessor()
	{
		this.jsonDecoder = new JsonDecoder<VimondEventAny>(VimondEventAny.class, ObjectMapperConfiguration.configurePretty());
	}
	
	public Decoder<VimondEventAny> getDecoderSingleton()
	{
		return jsonDecoder;
	}

	public boolean process(VimondEventAny message, int arg1)
	{
		LOG.info(toString() + " received message");
		try
		{
			consumer.putMessageIntoBuffer(message);
			return true;
		}
		catch(Exception e)
		{
			LOG.info(toString() + " error while processing the message ");
			e.printStackTrace();
			return false;
		}
	}
	
	public String toString()
	{
		return "FileSystemMessageProcessor: ";
	}
}
