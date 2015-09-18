package com.vimond.utils.functions.query;

import org.elasticsearch.client.Client;

public class MatchAll extends Query
{
	
	public MatchAll()
	{
		this.name = "Total number of documents";
	}

	@Override
	protected void execute(Client esClient, String index, boolean verbose)
	{
		this.searchResponse = esClient.prepareSearch(index)
		.setQuery(org.elasticsearch.index.query
		.QueryBuilders.matchAllQuery())
		.get();
		printResult();
	}

	@Override
	protected void printResult()
	{
		LOG.info(name + ": " + this.searchResponse.getHits().totalHits());
	}
	
}
