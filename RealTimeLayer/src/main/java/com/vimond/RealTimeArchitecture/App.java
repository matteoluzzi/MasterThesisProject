package com.vimond.RealTimeArchitecture;


import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import com.vimond.RealTimeArchitecture.Topology.StormTopologyBuilder;
import com.vimond.utils.config.AppProperties;

/**
 * Main class for the real-time layer implemented with Storm. It takes a configuration file for kafka connection and storm task configuration.</b>
 * Topology configuration is hardcoded with execution in localmode, for cluster execution it must be specified used the storm.yaml
 * @author matteoremoluzzi
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	ArgumentParser parser = ArgumentParsers.newArgumentParser("RealTimeArchitecture");
    	
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
