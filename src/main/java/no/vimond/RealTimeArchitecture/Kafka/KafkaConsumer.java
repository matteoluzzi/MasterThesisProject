package no.vimond.RealTimeArchitecture.Kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerService;
import com.vimond.common.kafka07.consumer.MessageProcessor;

public class KafkaConsumer extends KafkaConsumerService<String> implements KafkaConsumerStreamProducer 
{
	
	private static Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);
	
	private StormEventProcessor eventProcessor;

	public KafkaConsumer(MetricRegistry metricRegistry,
			HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig,
			KafkaConsumerConfig consumerConfig,
			MessageProcessor messageProcessor)
	{
		
		super(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig,
				messageProcessor);
		this.eventProcessor = (StormEventProcessor) messageProcessor;
	}


	public void shutdown()
	{
		if(this.isRunning.get())
		{
			try
			{
				this.stop();
				this.isRunning.set(false);
			} catch (Exception e)
			{
				LOG.error(null, e);
			}
		}
	}

	public String take()
	{
		try
		{
			return eventProcessor.getQueue().take();
		} catch (InterruptedException e)
		{
			LOG.error(null, e);
		}
		return null;
	}

	public void startConsuming()
	{
		if (!isRunning.get())
		{
			try
			{
				this.start();
				this.isRunning.set(true);
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

}
