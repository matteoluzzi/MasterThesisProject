package com.vimond.utils.functions.query;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vimond.utils.functions.ElasticSearchQueryInterface;

/**
 * Abstract class representing a query for elasticsearch. Each sub class must
 * implement the execute method.
 * 
 * @author matteoremoluzzi
 *
 */
public abstract class Query
{
	protected SearchResponse searchResponse;
	protected SummaryStatistics stats;
	protected static final Logger LOG = LoggerFactory.getLogger(Query.class);
	protected String name;

	public Query()
	{
		this.stats = new SummaryStatistics();
	}

	public void executeMultiple(ElasticSearchQueryInterface es, String index, long interval, long repetitions, boolean verbose, CyclicBarrier barrier)
	{
		while (--repetitions >= 0)
		{
			try
			{
				barrier.await();
				long end = es.getEndBatch();
				execute(es.esClient, index, verbose);
				long time = getTimeExecution();
				LOG.info(name + " execution: " + time + " ms" + " " + new DateTime());
				this.stats.addValue(time);
			
				Thread.sleep(end - System.currentTimeMillis());
				
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			catch (IllegalArgumentException e) 
			{
				LOG.error("Current query execution took more than " + interval  + " ms to complete, raise the inteval" );
				System.exit(-1);
			} catch (BrokenBarrierException e)
			{
				e.printStackTrace();
			}
			
			
		}
		printQueryStatistics();
	}

	public long getTimeExecution()
	{
		if (this.searchResponse != null)
		{
			return searchResponse.getTookInMillis();
		} else
			return -1;
	}

	public long getHits()
	{
		if (this.searchResponse != null)
		{
			return this.searchResponse.getHits().totalHits();
		} else
			return 0;
	}

	protected abstract void printQueryStatistics();

	protected abstract void execute(Client c, String index, boolean verbose);

	protected abstract void printResult();

}
