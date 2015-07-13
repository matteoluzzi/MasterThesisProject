package no.vimond.StorageArchitecture;

import java.io.IOException;

import com.backtype.hadoop.pail.Pail;

import no.vimond.StorageArchitecture.Consumer.KafkaConsumerHandler;

public class App
{

	public static void main(String[] args) throws IOException
	{

		try
		{
			Pail.create("/Users/matteoremoluzzi/myPailFolder", new MyPailStructure());
		} catch (IllegalArgumentException e)
		{
		}

		KafkaConsumerHandler handler = new KafkaConsumerHandler();

		handler.registerConsumerGroup();
		handler.startListening();

	}
}
