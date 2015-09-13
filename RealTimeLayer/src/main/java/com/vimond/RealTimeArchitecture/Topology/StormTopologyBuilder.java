package com.vimond.RealTimeArchitecture.Topology;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.vimond.RealTimeArchitecture.Bolt.ElasticSearchBolt;
import com.vimond.RealTimeArchitecture.Bolt.RouterBolt;
import com.vimond.RealTimeArchitecture.Bolt.UserAgentBolt;
import com.vimond.RealTimeArchitecture.Spout.SpoutCreator;
import com.vimond.utils.config.AppProperties;
import com.vimond.utils.data.Constants;

/**
 * Utility class for building a topology and run it according to the executor mode specified in the configuration file
 * @author matteoremoluzzi
 *
 */
public class StormTopologyBuilder
{
	private static Logger LOG = LoggerFactory
			.getLogger(StormTopologyBuilder.class);
	
	private static final long DEFAULT_RUNNING_TIME_IN_MS = 60000;

	private AppProperties props;

	public StormTopologyBuilder(AppProperties props)
	{
		this.props = props;
	}

	public void buildAndRun()
	{
		
		String es_index = this.props.getProperty("es_index") != null ? this.props.getProperty("es_index") : Constants.DEFAULT_ES_INDEX;
		boolean localMode = this.props.getProperty("localmode") != null ? Boolean.parseBoolean(this.props.getProperty("localmode")) : Constants.DEFAULT_LOCAL_MODE;
		boolean acking = this.props.getProperty("acking") != null ? Boolean.parseBoolean(this.props.getProperty("acking")) : Constants.DEFAULT_ACKING_MODE;
		
		
		TopologyBuilder builder = new TopologyBuilder();

		LOG.info("Creating topology components.....");
		
		int spout_tasks = this.props.getProperty("spout_tasks") != null ? Integer.parseInt(this.props.getProperty("spout_tasks")) : Constants.SPOUT_TASKS;
		int userAgent_tasks = this.props.getProperty("userAgent_tasks") != null ? Integer.parseInt(this.props.getProperty("userAgent_tasks")) : Constants.USER_AGENT_TASKS;
		int router_tasks = this.props.getProperty("router_tasks") != null ? Integer.parseInt(this.props.getProperty("router_tasks")) : Constants.ROUTER_TASKS;
		int elasticsearch_tasks = this.props.getProperty("el_tasks") != null ? Integer.parseInt(this.props.getProperty("el_tasks")) : Constants.EL_TASKS;
		
		/*
		 * Kafka Spout
		 */
		builder.setSpout(Constants.SPOUT, SpoutCreator.create(this.props), spout_tasks);
		
		/*
		 * Bolt in charge the tuple according to its fields
		 */
		builder.setBolt(Constants.BOLT_ROUTER, new RouterBolt(), router_tasks).shuffleGrouping(Constants.SPOUT);
		
		/*
		 * Bolt that breaks down the user agent into browser and os
		 */
		builder.setBolt(Constants.BOLT_USER_AGENT, new UserAgentBolt(), userAgent_tasks).shuffleGrouping(Constants.BOLT_ROUTER, Constants.UA_STREAM);
		
//		builder.setBolt(Constants.BOLT_GEOIP, new GeoLookUpBolt(), 6).shuffleGrouping(Constants.BOLT_ROUTER, Constants.IP_STREAM);

		/*
		 * Bolt that writes the result tuple to elasticsearch cluster
		 */
		builder.setBolt(Constants.BOLT_ES, new ElasticSearchBolt(es_index), elasticsearch_tasks)
		.shuffleGrouping(Constants.BOLT_USER_AGENT, Constants.UA_STREAM)
		.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 120); //flush data into ES every 30 seconds
		
		
		LOG.info("Creating topology components DONE");
		
		Config topConfig = null;
		
		//If localmode is enabled, use a custom zokeeper instance and load the hardcoded properties
		if(localMode)
		{
			topConfig = getTopologyConfiguration();
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("RealTimeTopology", topConfig, builder.createTopology());
			
			long time = (this.props.get("running_time") != null) ? Long.parseLong((String) this.props.get("running_time")) : DEFAULT_RUNNING_TIME_IN_MS;
			
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
		//else submit the topology to the cluster	
		else
		{
			topConfig = new Config();
			topConfig.put("acking", acking);
			try
			{
				StormSubmitter.submitTopology("RealTimeTopology", topConfig, builder.createTopology());
			} catch (AlreadyAliveException e)
			{
				e.printStackTrace();
			} catch (InvalidTopologyException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Load the hardcoded topology configuration for local mode execution
	 * @return
	 */
	public static Config getTopologyConfiguration()
	{
		LOG.info("Loading local mode properties");
		
		Config conf = new Config();
		
		conf.setDebug(false);
		conf.setNumWorkers(10);
	//	conf.setMessageTimeoutSecs(30);
	//	conf.setMaxSpoutPending(30000);
		conf.setNumAckers(0);
		conf.put("acking", "false");
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		
		//ElasticSearch bolt configuration
		conf.put("es.index.auto.create", "true");
		conf.put("es.nodes", "localhost");
		conf.put("es.port", "9200");
		conf.put("es.input.json", "true");
		conf.put("es.storm.bolt.write.ack", "false");
		conf.put("es.storm.bolt.flush.entries.size", 25000);
		conf.put("es.batch.size.entries", 25000);
		conf.put("es.batch.size.bytes", "100mb");
		conf.put("es.storm.bolt.tick.tuple.flush", "true");
		
		//custom serialization
		List<String> customSerializationClasses = new ArrayList<String>();
		customSerializationClasses.add("com.vimond.utils.data.StormEvent");
		conf.put("topology.kryo.register",customSerializationClasses);
		
		return conf;
	}

}
