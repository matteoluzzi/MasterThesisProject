package no.vimond.RealTimeArchitecture.Bolt;

import org.elasticsearch.storm.EsBolt;

import backtype.storm.tuple.Tuple;

public class ElasticSearchBolt extends EsBolt
{
	private static final long serialVersionUID = 1L;

	public ElasticSearchBolt(String target)
	{
		super(target);
	}

	@Override
	public void execute(Tuple input)
	{
		super.execute(input);
	}

}
