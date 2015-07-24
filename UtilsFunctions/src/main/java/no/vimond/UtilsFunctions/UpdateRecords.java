package no.vimond.UtilsFunctions;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.IndicesFilterBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateRecords
{
	private static final Logger LOG = LoggerFactory.getLogger(UpdateRecords.class);
	
	private Client esClient;
	private DateTimeFormatter formatter;

	public UpdateRecords()
	{
		@SuppressWarnings("resource")
		TransportClient transportClient = new TransportClient();
		
		this.esClient = transportClient.addTransportAddress(new InetSocketTransportAddress("172.24.1.229", 9300));
		this.formatter = DateTimeFormat.forPattern("yyyy-MM-dd/HH/mm");
	}

	public static void main(String[] args)
	{
		String path = args[0]; //something like YYYY-MM-DD/HH/mm
		int batchTimeInMinutes = Integer.parseInt(args[1]);
		
		LOG.info("Parameters: folder {}, timeframe {}", path, batchTimeInMinutes);
		

		UpdateRecords ur = new UpdateRecords();

		ur.deleteRecordsFromES(path, batchTimeInMinutes);

		ur.shutDown();
	}

	public void deleteRecordsFromES(String path, int timeFrameInMinutes)
	{
		//Usage of Search API to identify the matching ids and then perform a bulk delete operations
		
		BulkProcessor bp = this.getBulkProcessor(this.esClient);
		Map<String, Object> params = this.prepareQueryParameters(path, timeFrameInMinutes);
		
		if(params.size() == 2)
		{
			
			//Filter is better than a query because we need to filter on exact values, not interested in score matching
			FilterBuilder fb = new IndicesFilterBuilder(new RangeFilterBuilder("timestamp")
				.from(params.get("start_date"))
				.to(params.get("end_date"))
				.includeLower(false)
				.includeUpper(true), "storm").noMatchFilter("none");	
			
				
						/*		FilterBuilder fb = new AndFilterBuilder(new TypeFilterBuilder("player-events"), 
						new RangeFilterBuilder("timestamp")
						.from(params.get("start_date"))
						.to(params.get("end_date"))
						.includeLower(false)
						.includeUpper(true)
						);
					*/
			//search
			SearchResponse sr = this.esClient.prepareSearch()
					.setQuery(new MatchAllQueryBuilder())
					.setPostFilter(fb)
					.setScroll(new TimeValue(10000)) //keep the scroll context alive for 10 seconds
					.get();
			
			LOG.info("Search request completed in : " + sr.getTook());
			
			//scroll the result
			boolean scroll = true;
			while(scroll)
			{
				for(SearchHit hit : sr.getHits().getHits())
				{
					LOG.info("Found an hit: {}", hit.id());
					bp.add(new DeleteRequest(hit.getIndex(), hit.getType(), hit.getId()));
				}
				sr = this.esClient.prepareSearchScroll(sr.getScrollId())
					.setScroll(new TimeValue(10000)) //keep the scroll context alive for 10 seconds
					.get();
				
				if (sr.getHits().getHits().length == 0) { //no more results
			        scroll = false; 
			    }
			}
		}
		else
			LOG.error("Can't execute the delete operation: missing parameters");
		
		bp.close();
	}
	
	private BulkProcessor getBulkProcessor(Client client)
	{
		return BulkProcessor.builder(client, new BulkProcessor.Listener()
		{
			
			public void beforeBulk(long executionId, BulkRequest request)
			{
			}
			
			public void afterBulk(long executionId, BulkRequest request, Throwable failure)
			{
				LOG.error("Bulk requested failed: " + failure.getMessage());
				
			}
			
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response)
			{
				if(!response.hasFailures())
					LOG.info("Bulk requested completed in: {} ", response.getTook());
				
			}
		}).setBulkActions(1000)
		.setFlushInterval(TimeValue.timeValueSeconds(5))
		.build();
	}

	Map<String, Object> prepareQueryParameters(String dataFolder, int timeFrameInMinutes)
	{
		Map<String, Object> args = new HashMap<String, Object>();
		
    	try
    	{
    		DateTime time = this.formatter.parseDateTime(dataFolder);
    		args.put("start_date", time.getMillis()); //Java API requires a date in millis from the epoch
    		args.put("end_date", time.plusMinutes(timeFrameInMinutes).getMillis());
    		
    	}
    	catch(UnsupportedOperationException e)
    	{
    		LOG.error("Can't parse folder path");
    	}
    	catch(IllegalArgumentException e)
    	{
    		LOG.error("Can't parse folder path");
    	}
    	return args;
	}
	
	public void shutDown()
	{
		this.esClient.close();
	}
}
