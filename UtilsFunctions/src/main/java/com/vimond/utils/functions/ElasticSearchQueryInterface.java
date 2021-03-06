package com.vimond.utils.functions;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vimond.utils.config.AppProperties;
import com.vimond.utils.functions.query.Query;
import com.vimond.utils.functions.query.QueryFactory;
import com.vimond.utils.functions.query.QueryType;

/**
 * Class for executing a query and get statistics
 * 
 * @author matteoremoluzzi
 *
 */
public class ElasticSearchQueryInterface
{
	private static final Logger LOG = LoggerFactory.getLogger(UpdateRecords.class);

	public Client esClient;
	public ExecutorService executor;

	public long endBatch;
	public long interval;

	public ElasticSearchQueryInterface(String address)
	{
		@SuppressWarnings("resource")
		TransportClient transportClient = new TransportClient();
	
		this.esClient = transportClient.addTransportAddress(new InetSocketTransportAddress(address, 9300));
	}

	public static void main(String[] args)
	{

		ArgumentParser parser = ArgumentParsers.newArgumentParser("RealTimeArchitecture");

		parser.addArgument("-conf", "--configuration").help("Configuration file").type(String.class).required(true).dest("cfg");

		try
		{
			Namespace namespace = parser.parseArgs(args);
			String cfg_file = namespace.getString("cfg");

			AppProperties props = new AppProperties(cfg_file);
			
			ElasticSearchQueryInterface.startQueries(props);

		} catch (ArgumentParserException e1)
		{
			parser.printHelp();
		} catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	private static void startQueries(AppProperties props)
	{
		String[] queriesList = props.getProperty("queries").split(",");
		boolean verbose = Boolean.parseBoolean(props.getProperty("verbose", "true"));
		String index = props.getProperty("index", "");
		long interval = Long.parseLong(props.getProperty("interval", "0"));
		long numberOfExecution = Long.parseLong(props.getProperty("numberOfExecution", "1"));
		String address = props.getProperty("elasticsearch_address", "localhost");
		
		ElasticSearchQueryInterface esqi = new ElasticSearchQueryInterface(address);
		esqi.interval = interval;
		if(queriesList.length > 0)
		{
			esqi.executor = Executors.newFixedThreadPool(queriesList.length);
			CyclicBarrier barrier = new CyclicBarrier(queriesList.length, esqi.new UpdateInterval(esqi));
			esqi.endBatch = System.currentTimeMillis() + esqi.interval;
			for(String query : queriesList)
			{
				try
				{
					QueryType type = QueryType.valueOf(query);
					Query q = QueryFactory.getFactory().createQuery(type);
					esqi.executor.submit(new Runnable()
					{
						@Override
						public void run()
						{
							q.executeMultiple(esqi, index, interval, numberOfExecution, verbose, barrier);
						}
					});
				}
				catch(IllegalArgumentException e)
				{
					LOG.error(query + " cannot be resolved into a QueryType, going to exit now");
					System.exit(-1);
				}
			}
			esqi.executor.shutdown();
		}
		else
			LOG.error("Missing queries in the properties file");
	}
	
	public long getEndBatch()
	{
		return this.endBatch;
	}
	
	public void updateEndBatch(long interval)
	{
		this.endBatch += interval;
	}
	
	public Client getClient()
	{
		return this.esClient;
	}
	
	private class UpdateInterval implements Runnable
	{
		private ElasticSearchQueryInterface e;
		
		public UpdateInterval(ElasticSearchQueryInterface e)
		{
			this.e = e;
		}
		
		@Override
		public void run()
		{
			e.updateEndBatch(interval);
		}
	}
}
