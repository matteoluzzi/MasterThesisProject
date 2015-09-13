package com.vimond.utils.functions.query;

import java.io.Console;
import java.util.List;
import java.util.function.Consumer;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

public class TopPlayerStartEventsPerAsset extends Query
{

	public TopPlayerStartEventsPerAsset()
	{
		super();
	}

	@Override
	public void execute(Client esClient, String index)
	{
		FilterBuilder fb = new OrFilterBuilder(new TermFilterBuilder("eventName", "StartEventPerAsset"), new TermFilterBuilder("data.playerEvent", "str-start"));

		AbstractAggregationBuilder aggregations = AggregationBuilders
												.terms("by_assetName")
												.field("data.assetName")
												.size(50)
												.subAggregation(AggregationBuilders.sum("sum").field("counter"))
												.order(Terms.Order.aggregation("sum", false));
		if(index.equals(""))
		{
			this.searchResponse = esClient.prepareSearch()
					.setQuery(org.elasticsearch.index.query
					.QueryBuilders.filteredQuery(new MatchAllQueryBuilder(), fb))
					.setSize(50)
					.setPostFilter(fb)
					.addAggregation(aggregations).get();
		}
		else
		{
			this.searchResponse = esClient.prepareSearch(index)
					.setQuery(org.elasticsearch.index.query
					.QueryBuilders.filteredQuery(new MatchAllQueryBuilder(), fb))
					.setSize(50)
					.setPostFilter(fb)
					.addAggregation(aggregations).get();
		}
	}

	@Override
	public void printResult()
	{
		Terms byAssetName = this.searchResponse.getAggregations().get("by_assetName");
		List<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> buckets = byAssetName.getBuckets();
		buckets.forEach(new Consumer<Bucket>()
		{
			@Override
			public void accept(Bucket t)
			{
				Sum sum = t.getAggregations().get("sum");
				System.out.println(t.getKey() + " " + sum.getValue());
			}
		});
	}
}
