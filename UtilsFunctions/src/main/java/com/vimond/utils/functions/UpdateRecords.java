package com.vimond.utils.functions;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.IndicesFilterBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for deleting records from elasticsearch belonging to an index within a timeframe.<br>
 * Must be invoked by oozie coordinator when a batch process for the given timeframe has been correcty executed.
 * @author matteoremoluzzi
 *
 */
public class UpdateRecords
{
	private static final Logger LOG = LoggerFactory.getLogger(UpdateRecords.class);
	
	private Client esClient;
	private DateTimeFormatter formatter;

	public UpdateRecords(String es_address) throws Exception
	{
		@SuppressWarnings("resource")
		TransportClient transportClient = new TransportClient();
		this.esClient = transportClient.addTransportAddress(new InetSocketTransportAddress(es_address, 9300));
		this.formatter = DateTimeFormat.forPattern("yyyy-MM-dd/HH/mm").withZone(DateTimeZone.forID("Europe/Oslo"));
	}

	public static void main(String[] args)
	{
		if(args.length != 3)
		{
			LOG.error("Error with the paramenters: {}", (Object[]) args);
		}
		else
		{
			
			String path = args[0]; //something like hdfs://localhost:9000/user/matteoremoluzzi/dataset/master/YYYY-MM-DD/HH/mm
			int batchTimeInMinutes = Integer.parseInt(args[1]);
			
			String es_address = args[2];
			
			path = extractDate(path); //something like YYYY-MM-DD/HH/mm
			
			if(path != null)
			{
				LOG.info("Parameters: folder {}, timeframe {}, es {}", path, batchTimeInMinutes, es_address);
				
				try
				{
					UpdateRecords ur = new UpdateRecords(es_address);
					ur.deleteRecordsFromES(path, batchTimeInMinutes);
				}
				catch(Exception e)
				{
					LOG.error("Error while executing delete function: {}, {}", e.getClass(), e.getMessage());
				}
			}
			else
				LOG.error("Can't execute the action with this paramenters: folder {}, timeframe {}, es {}", path, batchTimeInMinutes, es_address);
		}
	}

	public void deleteRecordsFromES(String path, int timeFrameInMinutes) throws Exception
	{
		//Usage of Search API to identify the matching ids and then perform bulk delete operations
		
		BulkProcessor bp = this.getBulkProcessor(this.esClient);
		Map<String, Object> params = this.prepareQueryParameters(path, timeFrameInMinutes);
		
		if(params.size() == 2)
		{
			//Filter is better than a query because we need to filter on exact values, not interested in score matching
			FilterBuilder fb = new IndicesFilterBuilder(new RangeFilterBuilder("timestamp")
				.from(params.get("start_date"))
				.to(params.get("end_date"))
				.includeLower(false)
				.includeUpper(true), "vimond-realtime").noMatchFilter("none");	
			
			//search
			SearchResponse sr = this.esClient.prepareSearch()
					.setQuery(new MatchAllQueryBuilder())
					.setPostFilter(fb)
					.setScroll(new TimeValue(10000))
					.setSize(10000)//keep the scroll context alive for 10 seconds
					.get();
			
			LOG.info("Search request completed in : " + sr.getTook()  + " "+ sr.getHits().getTotalHits());
			
			//scroll the result
			boolean scroll = true;
			while(scroll)
			{
				for(SearchHit hit : sr.getHits().getHits())
				{
					bp.add(new DeleteRequest(hit.getIndex(), hit.getType(), hit.getId()));
				}
				sr = this.esClient.prepareSearchScroll(sr.getScrollId())
					.setScroll(new TimeValue(10000)) //keep the scroll context alive for 10 seconds
					.get();
				LOG.info("Search request completed in : " + sr.getTook());
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
				LOG.info("Going to execute new bulk composed of {} actions", request.numberOfActions());
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
		}).setBulkActions(10000)
		.setBulkSize(new ByteSizeValue(-1))
		.setConcurrentRequests(5)
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
	
	public static String extractDate(String input)
	{
		Pattern p = Pattern.compile("\\d{4}-\\d{2}-\\d{2}/\\d{2}/\\d{2}$");
		Matcher m = p.matcher(input);
		
		if(m.find())
			return m.group();
		else return null;
	}
}
