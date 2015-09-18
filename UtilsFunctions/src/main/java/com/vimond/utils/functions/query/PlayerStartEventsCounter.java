package com.vimond.utils.functions.query;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;

public class PlayerStartEventsCounter extends Query
{
	public PlayerStartEventsCounter()
	{
		super();
		this.name = "PlayerStartEventsCounter";
	}
	
	@Override
	public void execute(Client esClient, String index, boolean verbose)
	{
		FilterBuilder fb = new OrFilterBuilder(new TermFilterBuilder("eventName", "StartEventPerAsset"), new TermFilterBuilder("data.playerEvent", "str-start"));
		
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
		if(verbose)
			printResult();
								
	}

	@Override
	protected void printResult()
	{
		Sum sum = this.searchResponse.getAggregations().get("sum_of_counter");
		LOG.info("Number of player-start events = " + sum.getValue());
		
	}

}
