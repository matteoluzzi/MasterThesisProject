package no.vimond.RealTimeArchitecture.Spout;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import no.vimond.RealTimeArchitecture.Kafka.KafkaConsumer;
import no.vimond.RealTimeArchitecture.Kafka.StormEventProcessor;
import no.vimond.RealTimeArchitecture.Utils.Constants;
import no.vimond.RealTimeArchitecture.Utils.StormEvent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.vimond.common.kafka07.KafkaConfig;
import com.vimond.common.kafka07.consumer.KafkaConsumerConfig;

/**
 * Source spout for storm topology. It contains a <b>KafkaConsumer</b> which is
 * listening on the specified kafka topic<br>
 * The communication between <b>KafkaConsumer</b> and <b>KafkaSpout07</b> is
 * realized by a <code>BlockingQueue</code>
 * 
 * @author matteoremoluzzi
 *
 */
public class KafkaSpout07 implements IRichSpout
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LogManager.getLogger(KafkaSpout07.class);

	private SpoutOutputCollector collector;

	private KafkaConsumer consumer;
	private AtomicInteger count;
	private MetricRegistry metricRegistry;
	private HealthCheckRegistry healthCheckRegistry;
	private KafkaConfig kafkaConfig;
	private KafkaConsumerConfig consumerConfig;
	private AtomicInteger acked;
	private AtomicInteger failed;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector)
	{
		String zk_location = (String) conf.get("zk");
		String topic = (String) conf.get("topic");
		String consumer_group = (String) conf.get("consumer_group");

		this.collector = collector;
		this.count = new AtomicInteger(0);
		this.acked = new AtomicInteger(0);
		this.failed = new AtomicInteger(0);

		this.metricRegistry = new MetricRegistry();
		this.healthCheckRegistry = new HealthCheckRegistry();
		this.kafkaConfig = (zk_location != null) ? new KafkaConfig(zk_location)
				: new KafkaConfig(Constants.DEFAULT_ZK_LOCATION);
		this.consumerConfig = new KafkaConsumerConfig();
		this.consumerConfig.groupid = (consumer_group != null) ? consumer_group
				: Constants.DEFAULT_CONSUMER_GROUP_RT;
		this.consumerConfig.topic = (topic != null) ? topic
				: Constants.DEFAULT_TOPIC;
		this.consumerConfig.consumerThreads = 1;

		consumer = new KafkaConsumer(metricRegistry, healthCheckRegistry,
				kafkaConfig, consumerConfig, new StormEventProcessor());
		consumer.startConsuming();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("event"));
	}

	public void nextTuple()
	{
		StormEvent message = this.consumer.takeNoBlock();
		
		if(message != null)
		{
//			message.setInitTime(new DateTime().getMillis());
			Values output = new Values(message);
			UUID id = UUID.randomUUID();
			this.collector.emit(output, id);
			this.count.incrementAndGet();
	//		LOG.info("Received message: id = " + id + ", message = " + this.count.incrementAndGet() + " "
	//				+ message);
			this.consumer.addInProcessMessage(id, message);
			LOG.info("Processed message");
		}
		else
		{
			try
			{
				Thread.sleep(100);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

	public void ack(Object msgId)
	{
		if(msgId instanceof UUID)
		{
			UUID id = (UUID) msgId;
			LOG.debug(msgId + " acked");
			this.acked.incrementAndGet();
			this.consumer.handleAckedEvents(id);
		}		
	}

	public void fail(Object msgId)
	{
		if(msgId instanceof UUID)
		{
			UUID id = (UUID) msgId;
			this.failed.incrementAndGet();
			LOG.error(msgId + " failed, trying to recover it");
			this.consumer.handleFailedEvents(id);
		}		
	}
	
	public void close()
	{
		this.consumer.shutdown();
	//	LOG.debug("Shutting down....\nProcessed messages = " + this.count + "\nAcked messages = " + this.acked + "\nFailed messages = " + this.failed);
	}

	public void activate()
	{
		// TODO Auto-generated method stub
		
	}

	public void deactivate()
	{
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration()
	{
		// TODO Auto-generated method stub
		return null;
	}

}
