package com.vimond.RealTimeArchitecture.Kafka;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerService;
import com.vimond.common.kafka07.consumer.MessageProcessor;
import com.vimond.utils.data.StormEvent;

/**
 * Custom implementation of kafka consumer based on KafkaConsumerService implementation
 * @author matteoremoluzzi
 *
 */
public class KafkaConsumer extends KafkaConsumerService<VimondEventAny> implements KafkaConsumerStreamProducer 
{
	
	private static Logger LOG = LogManager.getLogger(KafkaConsumer.class);
	
	private StormEventProcessor eventProcessor;
	private Map<UUID, StormEvent> inProcessEvents;
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public KafkaConsumer(MetricRegistry metricRegistry,
			HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig,
			KafkaConsumerConfig consumerConfig,
			MessageProcessor messageProcessor)
	{
		
		super(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig,
				messageProcessor);
		this.eventProcessor = (StormEventProcessor) messageProcessor;
		this.inProcessEvents = new ConcurrentHashMap<UUID, StormEvent>();
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
				LOG.error(e);
			}
		}
	}

	public StormEvent take()
	{
		try
		{
			return eventProcessor.getQueue().remove();
		}
		catch(NoSuchElementException e)
		{
			return null;
		}
		
	}
	
	public StormEvent takeNoBlock()
	{
			return eventProcessor.getQueue().poll();
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
	
	public void handleFailedEvents(UUID id)
	{
		StormEvent event = this.inProcessEvents.remove(id);
		if(event != null)
		{
			this.eventProcessor.getQueue().add(event);
			LOG.debug("Event " + id + " successfully recovered");
		}
		else
			LOG.error("Error while recovering an event");
	}
	
	public void handleAckedEvents(UUID id)
	{
		this.inProcessEvents.remove(id);
		LOG.debug("Event " + id + " successfully processed");
	}
	
	public void addInProcessMessage(UUID id, StormEvent event)
	{
		this.inProcessEvents.put(id, event);
	}

}
