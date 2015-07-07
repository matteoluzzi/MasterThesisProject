package no.vimond.StorageArchitecture;


import no.vimond.StorageArchitecture.Consumer.KafkaConsumerHandler;


public class App
{
	
	public static void main(String[] args)
	{
		KafkaConsumerHandler handler = new KafkaConsumerHandler();
		
		handler.registerConsumerGroup();
		handler.startListening();
	
	}

}
