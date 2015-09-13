package com.vimond.utils.functions.query;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;

public class PlayerEndEventsCounter extends Query
{

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
		if(verbose)
			printResult();
								
	}

	@Override
	protected void printResult()
	{
		Sum sum = this.searchResponse.getAggregations().get("sum_of_counter");
		System.out.println("Number of player-end events = " + sum.getValue());
		
	}

}
