package no.vimond.UtilsFunctions;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchQueryInterface
{
	private static final Logger LOG = LoggerFactory.getLogger(UpdateRecords.class);

	private Client esClient;

	public ElasticSearchQueryInterface()
	{
		TransportClient transportClient = new TransportClient();

		this.esClient = transportClient.addTransportAddress(new InetSocketTransportAddress("172.24.1.229", 9300));
	}

	public static void main(String[] args)
	{
		ElasticSearchQueryInterface esqi = new ElasticSearchQueryInterface();
		
		SearchResponse sr = esqi.esClient.prepareSearch("vimond-realtime")
				.addAggregation(AggregationBuilders.dateHistogram("by_date").field("timestamp").interval(DateHistogram.Interval.YEAR))
				.get();
		
		sr.getHits();

	}

}
