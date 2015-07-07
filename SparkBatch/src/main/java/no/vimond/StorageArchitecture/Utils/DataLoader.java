package no.vimond.StorageArchitecture.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.jobhistory.Events;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class DataLoader implements Serializable
{	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DataLoader()
	{
	}
	
	public JavaRDD<StormEvent> loadEventsFromFiles(JavaSparkContext ctx)
	{
		
		
		JavaRDD<String> rdd = ctx.textFile(Constants.MESSAGE_PATH + "*.txt",3);
		JavaRDD<StormEvent> events_rdd = rdd.mapPartitions(new ParseJsonEvents());
		
		return events_rdd;
	}
	
	public class ParseJsonEvents implements FlatMapFunction<Iterator<String>, StormEvent>
	{

		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<StormEvent> call(Iterator<String> str_events) throws Exception
		{
			ObjectMapper mapper = ObjectMapperConfiguration.configure();
			ArrayList<StormEvent> events = new ArrayList<StormEvent>();
			while(str_events.hasNext())
			{
				StormEvent event = mapper.readValue(str_events.next(), StormEvent.class);
				events.add(event);
			}
			
			return events;
		}
		
	}
	
}
