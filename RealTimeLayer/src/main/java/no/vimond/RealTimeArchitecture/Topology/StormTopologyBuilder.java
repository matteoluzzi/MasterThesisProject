package no.vimond.RealTimeArchitecture.Topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import no.vimond.RealTimeArchitecture.Bolt.ElasticSearchBolt;
import no.vimond.RealTimeArchitecture.Bolt.GeoLookUpBolt;
import no.vimond.RealTimeArchitecture.Bolt.SimpleBolt;
import no.vimond.RealTimeArchitecture.Spout.SpoutCreator;
import no.vimond.RealTimeArchitecture.Utils.Constants;

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

	public void buildAndRun(Map<String, String> args)
	{
		TopologyBuilder builder = new TopologyBuilder();

		LOG.info("Creating topology components.....");
		
		builder.setSpout("kafka-spout", SpoutCreator.create(args), 8);
		
//		builder.setBolt("test-bolt", new TestBolt(), 1).shuffleGrouping("kafka-spout");

		builder.setBolt("simple-bolt", new SimpleBolt(), 3).shuffleGrouping(
				"kafka-spout");
		
		builder.setBolt("geo-bolt", new GeoLookUpBolt(), 2).shuffleGrouping("simple-bolt", Constants.IP_STREAM);
		
		builder.setBolt("el-bolt", new ElasticSearchBolt("storm/player-events"), 2)
		.shuffleGrouping("simple-bolt")
		.shuffleGrouping("geo-bolt", Constants.IP_STREAM)
		.addConfiguration("es.storm.bolt.write.ack", true)
		.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 120); //flush data into ES every 2 minutes
		
		builder.setBolt("el-bolt1", new ElasticSearchBolt("storm/player-log-events"), 1)
		.shuffleGrouping("simple-bolt")
		.addConfiguration("es.storm.bolt.write.ack", true)
		.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 120); //flush data into ES every 2 minutes
		
		LOG.info("Creating topology components DONE");

		Config topConfig = getTopologyConfiguration();
		
		topConfig.putAll(args);

		LOG.info("Launching topology");
		/*	
		try
		{
			StormSubmitter.submitTopology("RealTimeTopology", topConfig, builder.createTopology());
		} catch (AlreadyAliveException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/
		LocalCluster cluster = new LocalCluster("localhost", new Long(2181));
		cluster.submitTopology("RealTimeTopology", topConfig,
				builder.createTopology());
		
		long time = (this.args.get("running_time") != null) ? Long.parseLong(this.args.get("running_time")) : DEFAULT_RUNNING_TIME_IN_MS;
		
		try
		{
			Thread.sleep(time * 1000 * 60);
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		finally
		{
			cluster.shutdown();
		}
	}
	
	public static Config getTopologyConfiguration()
	{
		Config conf = new Config();
		
		conf.setDebug(true);
		conf.setNumWorkers(10);
		conf.setNumAckers(10);
		conf.setMessageTimeoutSecs(20);
		
		//ElasticSearch bolt configuration
		conf.put("es.storm.spout.reliable.queue.size", 500);
		conf.put("es.storm.spout.reliable", true);
		conf.put("es.storm.spout.reliable.handle.tuple.failure", "warn");
		conf.put("es.index.auto.create", "true");
		conf.put("es.nodes", "localhost");
		conf.put("es.port", "9200");
		conf.put("es.input.json", "true");
		conf.put("es.storm.bolt.flush.entries.size", 100);
		
		//custom serialization
		List<String> customSerializationClasses = new ArrayList<String>();
		customSerializationClasses.add("no.vimond.RealTimeArchitecture.Utils.StormEvent");
		conf.put("topology.kryo.register",customSerializationClasses);
		
		return conf;
	}

}
