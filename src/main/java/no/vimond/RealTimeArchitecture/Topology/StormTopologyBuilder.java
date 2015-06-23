package no.vimond.RealTimeArchitecture.Topology;

import java.util.Map;

import no.vimond.RealTimeArchitecture.Bolt.SimpleBolt;
import no.vimond.RealTimeArchitecture.Spout.SpoutCreator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class StormTopologyBuilder
{
	private static Logger LOG = LoggerFactory
			.getLogger(StormTopologyBuilder.class);
	
	private static final long DEFAULT_RUNNING_TIME_IN_MS = 60000;

	private Map<String, String> args;

	public StormTopologyBuilder(Map<String, String> args)
	{
		this.args = args;
	}

	public void buildAndRun()
	{
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("kafka-spout", SpoutCreator.create(args), 1);

		builder.setBolt("simple-bolt", new SimpleBolt(), 3).shuffleGrouping(
				"kafka-spout");

		Config topConfig = new Config();
		topConfig.setDebug(true);
		topConfig.setNumWorkers(2);
		topConfig.setMessageTimeoutSecs(10);

		LocalCluster cluster = new LocalCluster("localhost", new Long(2181));
		cluster.submitTopology("RealTimeTopology", topConfig,
				builder.createTopology());
		
		long time = (this.args.get("running_time") != null) ? Long.parseLong(this.args.get("running_time")) : DEFAULT_RUNNING_TIME_IN_MS;
		
		try
		{
			Thread.sleep(time);
			cluster.shutdown();
		} catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
