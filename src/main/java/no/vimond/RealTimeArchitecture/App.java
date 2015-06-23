package no.vimond.RealTimeArchitecture;

import java.util.HashMap;
import java.util.Map;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import no.vimond.RealTimeArchitecture.Topology.StormTopologyBuilder;


public class App 
{
    public static void main( String[] args )
    {
    	ArgumentParser parser = ArgumentParsers.newArgumentParser("KafkaProducer");
		parser.addArgument("-z", "--zookeeper").help("Zookeeper node address and port").type(String.class).required(false).dest("zookeeper");
		parser.addArgument("-t", "--topic").help("Topic where to write the message").type(String.class).required(false).dest("topic");
		parser.addArgument("-a", "--kafkaAPI").help("Version of Kafka API").type(String.class).required(false).dest("api");
		
		try
		{
			Namespace namespace = parser.parseArgs(args);
			String zkHost = namespace.getString("zookeeper");
			String topic = namespace.getString("topic");
			String api = namespace.getString("api");
			
			Map<String, String> arguments = new HashMap<String, String>();
			if(zkHost != null)
				arguments.put("zkHost", zkHost);
			if(topic != null)
				arguments.put("topic", topic);
			if(api != null)
				arguments.put("kafka_api_version", api);
			
			StormTopologyBuilder topologyBuilder = new StormTopologyBuilder(arguments);
			
			topologyBuilder.buildAndRun();
			
		} catch (ArgumentParserException e1)
		{
			parser.printHelp();
		}	
		catch (Exception e)
		{
			e.printStackTrace();
		}

    }
}
