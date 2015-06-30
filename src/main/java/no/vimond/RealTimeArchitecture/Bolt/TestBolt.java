package no.vimond.RealTimeArchitecture.Bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TestBolt implements IRichBolt
{

	private static Logger LOG = LoggerFactory.getLogger(TestBolt.class);
	private static final long serialVersionUID = -6631908055039600737L;
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector)
	{
		this.collector = collector;
	}

	public void execute(Tuple input)
	{
		LOG.info("Just ignoring every received message");
		//collector.fail(input);
		
	}

	public void cleanup()
	{

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{

	}

	public Map<String, Object> getComponentConfiguration()
	{
		// TODO Auto-generated method stub
		return null;
	}

}
