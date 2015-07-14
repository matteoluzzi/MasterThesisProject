package no.vimond.StorageArchitecture;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import no.vimond.StorageArchitecture.Consumer.KafkaConsumerHandler;
import no.vimond.StorageArchitecture.PailStructure.EventPailStructure;
import no.vimond.StorageArchitecture.PailStructure.TimeFramePailStructure;
import no.vimond.StorageArchitecture.Utils.Constants;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.SequenceFileFormat;

public class App
{

	public static void main(String[] args) throws IOException
	{

		try
		{
		/*	Map<String, Object> options = new HashMap<String, Object>();
			options.put(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_GZIP);
			options.put(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK);
			Pail.create(Constants.NEW_DATA_PATH, new PailSpec("SequenceFile", options, new TimeFramePailStructure()));
			 */
			Pail.create(Constants.NEW_DATA_PATH, new TimeFramePailStructure());
		} catch (IllegalArgumentException e)
		{
		}

		KafkaConsumerHandler handler = new KafkaConsumerHandler();

		handler.registerConsumerGroup();
		handler.startListening();

	}
}
