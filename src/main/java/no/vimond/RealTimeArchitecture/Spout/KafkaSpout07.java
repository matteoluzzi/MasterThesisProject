package no.vimond.RealTimeArchitecture.Spout;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import no.vimond.RealTimeArchitecture.Kafka.KafkaConsumer;
import no.vimond.RealTimeArchitecture.Kafka.StormEventProcessor;
import no.vimond.RealTimeArchitecture.Utils.Constants;
import no.vimond.RealTimeArchitecture.Utils.StormEvent;

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

/**
 * Source spout for storm topology. It contains a <b>KafkaConsumer</b> which is listening on the specified kafka topic<br>
 * The communication between <b>KafkaConsumer</b> and <b>KafkaSpout07</b> is realized by a <code>BlockingQueue</code>
 * @author matteoremoluzzi
 *
 */
public class KafkaSpout07 extends BaseRichSpout
{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(KafkaSpout07.class);

	private SpoutOutputCollector collector;
	
	private KafkaConsumer consumer;
	private AtomicInteger count;
	private MetricRegistry metricRegistry;
	private HealthCheckRegistry healthCheckRegistry;
	private StormEventProcessor messageProcessor;
	private KafkaConfig kafkaConfig;
	private KafkaConsumerConfig consumerConfig;

	
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector)
	{
		String zk_location = (String) conf.get("zk");
		String topic = (String) conf.get("topic");
		String consumer_group = (String) conf.get("consumer_group");
		
		this.collector = collector;
		this.count = new AtomicInteger(0);
		
		this.metricRegistry = new MetricRegistry();
		this.healthCheckRegistry = new HealthCheckRegistry();
		this.messageProcessor = new StormEventProcessor();
		this.kafkaConfig = (zk_location != null) ? new KafkaConfig(zk_location) : new KafkaConfig(Constants.DEFAULT_ZK_LOCATION);
		this.consumerConfig = new KafkaConsumerConfig();
		this.consumerConfig.groupid = (consumer_group != null) ? consumer_group : Constants.DEFAULT_CONSUMER_GROUP;
		this.consumerConfig.topic = (topic != null) ? topic : Constants.DEFAULT_TOPIC;
		this.consumerConfig.consumerThreads = 2;
		
		
		consumer = new KafkaConsumer(metricRegistry, healthCheckRegistry,
				kafkaConfig, consumerConfig, messageProcessor);
		consumer.startConsuming();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("event"));
	}

	public void nextTuple()
	{
		StormEvent message = this.consumer.take();
		Values output = new Values(message);
		this.collector.emit(output, UUID.randomUUID());
		LOG.info("Received message " + this.count.getAndIncrement() + " "
				+ message);
	}

	@Override
	public void close()
	{
		if (consumer != null)
			consumer.shutdown();
	}

}
