package com.vimond.utils.functions.query;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class representing a query for elasticsearch. Each sub class must implement the execute method.
 * @author matteoremoluzzi
 *
 */
public abstract class Query
{
	protected SearchResponse searchResponse;
	protected ExecutorService executor;
	protected SummaryStatistics stats;
	protected static final Logger LOG = LoggerFactory.getLogger(Query.class);
	
	
	public Query()
	{
		this.executor = Executors.newFixedThreadPool(5);
		this.stats = new SummaryStatistics();
	}
	
	public void executeMultiple(Client c, String index, long interval, long repetitions, boolean verbose)
	{
		while(--repetitions >= 0)
		{
			this.executor.submit(new Runnable()
			{
				@Override
				public void run()
				{
					execute(c, index, verbose);
					long time = getTimeExecution();
					LOG.info("Execution: " + time + " ms" + " " + new DateTime());
					stats.addValue(time);
				}
			});
			try
			{
				Thread.sleep(interval);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
		printQueryStatistics();
		
		this.executor.shutdown();
	}
	
	public long getTimeExecution() 
	{
		if(this.searchResponse != null)
		{
			return searchResponse.getTookInMillis();
		}
		else return -1;
	}
	
	private void printQueryStatistics()
	{
		LOG.info("Results: \nAvg time = " + stats.getMean() + "\nMax time: " + stats.getMax() + "\nMin time: " + stats.getMin());
	}
	
	protected abstract void execute(Client c, String index, boolean verbose);
	
	protected abstract void printResult();

}
