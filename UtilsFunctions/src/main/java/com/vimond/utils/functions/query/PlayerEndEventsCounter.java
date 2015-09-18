package com.vimond.utils.functions.query;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlayerEndEventsCounter extends Query
{
	private static final Logger fileLog = LoggerFactory.getLogger("player_ends_counter");
	
	public PlayerEndEventsCounter()
	{
		super();
		this.name = "PlayerEndEventsCounter";
	}

	@Override
	public void execute(Client esClient, String index, boolean verbose)
	{
		FilterBuilder fb = new OrFilterBuilder(new TermFilterBuilder("eventName", "StopEventPerAsset"), new TermFilterBuilder("data.playerEvent", "end"));
		
		SumBuilder aggregation = AggregationBuilders.sum("sum_of_counter")
								.field("counter");
		
		if(index.equals(""))
		{
			this.searchResponse = esClient.prepareSearch()
					.setQuery(org.elasticsearch.index.query
					.QueryBuilders.filteredQuery(new MatchAllQueryBuilder(), fb))
					.setSize(50)
					.setPostFilter(fb)
					.addAggregation(aggregation).get();
		}
		else
		{
			this.searchResponse = esClient.prepareSearch(index)
					.setQuery(org.elasticsearch.index.query
					.QueryBuilders.filteredQuery(new MatchAllQueryBuilder(), fb))
					.setSize(50)
					.setPostFilter(fb)
					.addAggregation(aggregation).get();
		}
		fileLog.info(String.valueOf(this.searchResponse.getTookInMillis()));
		if(verbose)
			printResult();
								
	}

	@Override
	protected void printResult()
	{
		Sum sum = this.searchResponse.getAggregations().get("sum_of_counter");
		LOG.info("Number of player-end events = " + sum.getValue());
		
	}

	@Override
	protected void printQueryStatistics()
	{
		LOG.info(name + " results: \nAvg time = " + stats.getMean() + " ms\nMax time: " + stats.getMax() + " ms\nMin time: " + stats.getMin() + " ms");
		fileLog.info(name + " results: \nAvg time = " + stats.getMean() + " ms\nMax time: " + stats.getMax() + " ms\nMin time: " + stats.getMin() + " ms");
	}
}
