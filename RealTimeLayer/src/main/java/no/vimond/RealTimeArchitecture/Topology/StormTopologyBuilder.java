package no.vimond.RealTimeArchitecture.Topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import no.vimond.RealTimeArchitecture.Bolt.ElasticSearchBolt;
import no.vimond.RealTimeArchitecture.Bolt.GeoLookUpBolt;
import no.vimond.RealTimeArchitecture.Bolt.SimpleBolt;
import no.vimond.RealTimeArchitecture.Spout.SpoutCreator;
import no.vimond.RealTimeArchitecture.Utils.AppProperties;
import no.vimond.RealTimeArchitecture.Utils.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

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
		
		TopologyBuilder builder = new TopologyBuilder();

		LOG.info("Creating topology components.....");
		
		builder.setSpout(Constants.SPOUT, SpoutCreator.create(this.props), 6);
		
		builder.setBolt(Constants.BOLT_ROUTER, new SimpleBolt(), 10).shuffleGrouping(Constants.SPOUT);
		
		builder.setBolt(Constants.BOLT_GEOIP, new GeoLookUpBolt(), 10).shuffleGrouping(Constants.BOLT_ROUTER, Constants.IP_STREAM);
		
		builder.setBolt(Constants.BOLT_ES, new ElasticSearchBolt(es_index), 10)
		.shuffleGrouping(Constants.BOLT_ROUTER)
		.shuffleGrouping(Constants.BOLT_GEOIP, Constants.IP_STREAM)
		.addConfiguration("es.storm.bolt.write.ack", false)
		.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 120); //flush data into ES every 2 minutes
		
		
		LOG.info("Creating topology components DONE");
		
		Config topConfig = null;
		
		if(localMode)
		{
			topConfig = getTopologyConfiguration();
			
			String zKLocation = (props.getProperty("zookeeper.connect") != null) ? props.getProperty("zookeeper.connect") : Constants.DEFAULT_ZK_LOCATION;
			long zKPort = props.getProperty("zookeeper.port") != null ? Long.parseLong(props.getProperty("zookeeper.port")) : Constants.DEFAULT_ZK_PORT;
			
			LocalCluster cluster = new LocalCluster(zKLocation, zKPort);
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
			
		else
		{
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
		}
	}
	
	public static Config getTopologyConfiguration()
	{
		Config conf = new Config();
		
		conf.setDebug(true);
		conf.setNumWorkers(6);
		conf.setMessageTimeoutSecs(30);
		conf.setMaxSpoutPending(5000);
		conf.setNumAckers(0);
		
		//ElasticSearch bolt configuration
		conf.put("es.index.auto.create", "true");
		conf.put("es.nodes", "localhost");
		conf.put("es.port", "9200");
		conf.put("es.input.json", "true");
		conf.put("es.storm.bolt.flush.entries.size", 1000);
		
		//custom serialization
		List<String> customSerializationClasses = new ArrayList<String>();
		customSerializationClasses.add("no.vimond.RealTimeArchitecture.Utils.StormEvent");
		conf.put("topology.kryo.register",customSerializationClasses);
		
		return conf;
	}

}
