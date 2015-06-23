package no.vimond.RealTimeArchitecture.Bolt;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SimpleBolt implements IRichBolt
{
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(SimpleBolt.class);

	private AtomicInteger count;
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector)
	{
		this.count = new AtomicInteger(0);
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("message"));

	}

	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}

	public void execute(Tuple input)
	{
		String message = input.getString(0);
		LOG.info("Received message " + this.count.getAndIncrement() + " "
				+ message);
		collector.emit(new Values(message));
		collector.ack(input);
	}

	public void cleanup()
	{
		LOG.warn("Going to sleep now");
	}

}
