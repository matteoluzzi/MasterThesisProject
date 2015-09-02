package com.vimond.StorageArchitecture.Jobs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.joda.time.DateTime;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.utils.data.SimpleModel;
import com.vimond.utils.data.SparkEvent;

public class EndEventsPerAssetJob extends WorkingJob<SparkEvent>
{

	private static final long serialVersionUID = -5435852771107579884L;
	
	public EndEventsPerAssetJob(JavaRDD<SparkEvent> inputDataset, DateTime timestamp, String timewindow)
	{
		super(inputDataset, timestamp, timewindow);
	}

	@Override
	public void run(JavaSparkContext ctx)
	{
		
		JavaRDD<SparkEvent> endEventsRdd = this.inputDataset.filter(new Function<SparkEvent, Boolean>()
		{
			private static final long serialVersionUID = -3788210390379228766L;
			
			@Override
			public Boolean call(SparkEvent event) throws Exception
			{
				String playerEvent = event.getPlayerEventType();
				if (playerEvent != null && playerEvent.equals("end"))
				{
					return true;
				}
				else
					return false;
			}
		});
		
		JavaPairRDD<String, Integer> mappedRdd = endEventsRdd.mapToPair(t -> new Tuple2<String, Integer>(t.getAssetName(), 1));
		
		mappedRdd = mappedRdd.reduceByKey((x,y) -> x+y);
		
		JavaRDD<String> result = mappedRdd.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Integer>>, String>()
		{
			private static final long serialVersionUID = 2967559442626319794L;

			@Override
			public Iterable<String> call(Iterator<Tuple2<String, Integer>> tuples) throws Exception
			{
				
				ObjectMapper mapper = new ObjectMapper();
				mapper.registerModule(new JodaModule());
				
				Collection<String> result = new ArrayList<String>();
				
				while(tuples.hasNext())
				{
					Tuple2<String, Integer> t = tuples.next();
					
					SimpleModel counterByAsset = new SimpleModel();
					counterByAsset.setOriginator("VimondAnalytics");
					counterByAsset.setRandomGuid();
					counterByAsset.setTenant("tv2 Sumo");
					counterByAsset.eventName = "StopEventPerAsset";
					counterByAsset.setTimestamp(timestamp);
					counterByAsset.setGenericValue("timewindow", timewindow);
					counterByAsset.setGenericValue("data.assetName", t._1());
					counterByAsset.setGenericValue("counter", t._2());
					String event = mapper.writeValueAsString(counterByAsset);
					result.add(event);
				}
				
				return result;
			}
		});
		
		JavaEsSpark.saveToEs(result, "vimond-batch/batch-endEventsByAsset");
	}
}
