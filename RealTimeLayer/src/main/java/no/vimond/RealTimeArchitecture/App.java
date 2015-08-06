package no.vimond.RealTimeArchitecture;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import no.vimond.RealTimeArchitecture.Topology.StormTopologyBuilder;
import no.vimond.RealTimeArchitecture.Utils.AppProperties;


public class App 
{
    public static void main( String[] args )
    {
    	ArgumentParser parser = ArgumentParsers.newArgumentParser("KafkaProducer");
    	
    	parser.addArgument("-conf", "--configuration").help("Configuration time").type(String.class).required(true).dest("cfg");
    	
		try
		{
			Namespace namespace = parser.parseArgs(args);
			String cfg_file = namespace.getString("cfg");
			
			AppProperties props = new AppProperties(cfg_file);
			
			StormTopologyBuilder topologyBuilder = new StormTopologyBuilder(props);
			
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
