package com.vimond.utils.functions;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vimond.utils.functions.query.Query;
import com.vimond.utils.functions.query.QueryFactory;
import com.vimond.utils.functions.query.QueryType;

/**
 * Class for executing a query and get statistics
 * @author matteoremoluzzi
 *
 */
public class ElasticSearchQueryInterface
{
	private static final Logger LOG = LoggerFactory.getLogger(UpdateRecords.class);

	private Client esClient;
	

	public ElasticSearchQueryInterface()
	{
		TransportClient transportClient = new TransportClient();

		this.esClient = transportClient.addTransportAddress(new InetSocketTransportAddress("52.18.78.197", 9300));
	}

	public static void main(String[] args)
	{
		ElasticSearchQueryInterface esqi = new ElasticSearchQueryInterface();
		
		Query q = QueryFactory.getFactory().createQuery(QueryType.TOP_ASSET_START);
		q.executeMultiple(esqi.esClient, "", 30000, 90);
		
	}
	
	
	
	
	
}
