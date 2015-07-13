package no.vimond.StorageArchitecture;

import java.io.IOException;

import com.backtype.hadoop.pail.Pail;

import no.vimond.StorageArchitecture.Consumer.KafkaConsumerHandler;
import no.vimond.StorageArchitecture.PailStructure.TimeFramePailStructure;
import no.vimond.StorageArchitecture.Utils.Constants;

public class App
{

	public static void main(String[] args) throws IOException
	{

		try
		{
			Pail.create(Constants.NEW_DATA_PATH, new TimeFramePailStructure());
		} catch (IllegalArgumentException e)
		{
		}

		KafkaConsumerHandler handler = new KafkaConsumerHandler();

		handler.registerConsumerGroup();
		handler.startListening();

	}
}
