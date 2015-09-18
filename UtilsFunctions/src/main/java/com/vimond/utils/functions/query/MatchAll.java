package com.vimond.utils.functions.query;

import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatchAll extends Query
{
	private static final Logger fileLog = LoggerFactory.getLogger("match_all");
	
	public MatchAll()
	{
		super();
		this.name = "Total number of documents";
	}

	@Override
	protected void execute(Client esClient, String index, boolean verbose)
	{
		this.searchResponse = esClient.prepareSearch()
		.setQuery(org.elasticsearch.index.query
		.QueryBuilders.matchAllQuery())
		.get();
		printResult();
	}

	@Override
	protected void printResult()
	{
		LOG.info(name + ": " + this.searchResponse.getHits().totalHits());
		fileLog.info(String.valueOf(this.searchResponse.getHits().totalHits()));
	}

	@Override
	protected void printQueryStatistics()
	{
		LOG.info(name + " results: \nAvg time = " + stats.getMean() + " ms\nMax time: " + stats.getMax() + " ms\nMin time: " + stats.getMin() + " ms");
		fileLog.info(name + " results: \nAvg time = " + stats.getMean() + " ms\nMax time: " + stats.getMax() + " ms\nMin time: " + stats.getMin() + " ms");
	}
	
}
