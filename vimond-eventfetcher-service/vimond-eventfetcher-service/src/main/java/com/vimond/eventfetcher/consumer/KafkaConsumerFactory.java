package com.vimond.eventfetcher.consumer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;
import com.vimond.common.messages.MessageConsumer;
import com.vimond.eventfetcher.configuration.ProcessorConfiguration;

import com.vimond.eventfetcher.processor.BatchProcessor;

/**
 * Factory for crating a new KafkaConsumer. It s a MessageConsumer instance
 * @author matteoremoluzzi
 *
 */
public class KafkaConsumerFactory
{
	private static KafkaConsumerFactory instance = new KafkaConsumerFactory();

	private KafkaConsumerFactory()
	{
	}

	public static KafkaConsumerFactory getFactory()
	{
		return instance;
	}

	@SuppressWarnings("rawtypes")
	public MessageConsumer createMessageProcessor(KafkaConsumerEnum type, MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry, KafkaConfig kafkaConfig, KafkaConsumerConfig consumerConfig, BatchProcessor fsProcessor, ProcessorConfiguration conf)
	{
		switch (type)
		{
		case UNRELIABLE:
			return new UnreliableKafkaConsumerService(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, fsProcessor, conf);
		case RELIABLE:
			return new ReliableKafkaConsumer(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, fsProcessor, conf);
		default:
			return new UnreliableKafkaConsumerService(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, fsProcessor, conf);
		}
	}
}
