package no.vimond.RealTimeArchitecture.Spout;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import no.vimond.RealTimeArchitecture.Kafka.KafkaConsumer;
import no.vimond.RealTimeArchitecture.Kafka.StormEventProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;

public class KafkaSpout07 extends BaseRichSpout
{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(KafkaSpout07.class);
	
	private SpoutOutputCollector collector;
	private KafkaConsumer consumer;
	private AtomicInteger count;
	
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector)
	{
		this.collector = collector;
		this.count = new AtomicInteger(0);
		
		MetricRegistry metricRegistry = new MetricRegistry();
		HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();
		StormEventProcessor messageProcessor = new StormEventProcessor();
		KafkaConfig kafkaConfig = new KafkaConfig("localhost:2181");
		KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig();
		consumerConfig.groupid = "kafka-consumer";
		consumerConfig.topic = "player-events";
		
		consumer = new KafkaConsumer(metricRegistry, healthCheckRegistry, kafkaConfig, consumerConfig, messageProcessor);
		consumer.startConsuming();
	}

	

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("event"));	
	}

	public void nextTuple()
	{
		String message = this.consumer.take();
		Values output = new Values(message);
		this.collector.emit(output, UUID.randomUUID());	
		LOG.info("Received message " + this.count.getAndIncrement() + " " + message);
	}
	
	@Override
	public void close()
	{
		if(consumer != null)
			consumer.shutdown();
	}
	


}
