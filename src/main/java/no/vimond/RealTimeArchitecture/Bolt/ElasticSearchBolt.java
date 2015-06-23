package no.vimond.RealTimeArchitecture.Bolt;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class ElasticSearchBolt extends BaseBasicBolt
{

	private static final long serialVersionUID = 1L;


	@Override
	public void prepare(Map stormConf, TopologyContext context)
	{
		
	}
	
	public void execute(Tuple input, BasicOutputCollector collector)
	{
		
		
	}

	
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		
		
	}

}
