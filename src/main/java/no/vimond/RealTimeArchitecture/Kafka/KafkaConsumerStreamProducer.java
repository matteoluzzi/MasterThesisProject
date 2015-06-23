package no.vimond.RealTimeArchitecture.Kafka;

public interface KafkaConsumerStreamProducer
{
	public void startConsuming();
	
	public void shutdown();
	
	public String take();
	
}
