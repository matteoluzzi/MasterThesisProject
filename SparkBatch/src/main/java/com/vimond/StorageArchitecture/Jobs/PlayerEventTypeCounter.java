package com.vimond.StorageArchitecture.Jobs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.joda.time.DateTime;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.utils.data.SimpleModel;
import com.vimond.utils.data.SparkEvent;

public class PlayerEventTypeCounter extends WorkingJob<SparkEvent>
{
	private static final long serialVersionUID = 2723836746491597580L;

	public PlayerEventTypeCounter(JavaRDD<SparkEvent> inputDataset, DateTime timestamp, String timewindow)
	{
		super(inputDataset, timestamp, timewindow);
	}

	@Override
	public void run(JavaSparkContext ctx)
	{
		
		JavaRDD<SparkEvent> playerEvents = this.inputDataset.filter(new Function<SparkEvent, Boolean>()
		{
			private static final long serialVersionUID = 3559149421471691273L;

			@Override
			public Boolean call(SparkEvent e) throws Exception
			{
				String playerEvent = e.getPlayerEventType();
				if(playerEvent != null && (playerEvent.equals("str-start") || playerEvent.equals("end")))
					return true;
				else return false;
			}
		});
		
		JavaPairRDD<String, Integer> playerEventsTuple = playerEvents.mapToPair(new PairFunction<SparkEvent, String, Integer>()
		{
			private static final long serialVersionUID = -4297209944982407763L;

			@Override
			public Tuple2<String, Integer> call(SparkEvent t) throws Exception
			{
				Tuple2<String, Integer> output = new Tuple2<String, Integer>(t.getPlayerEventType(), 1);
				return output;
			}
		});
		
		playerEventsTuple = playerEventsTuple.reduceByKey((x, y) -> x+y);
		
		JavaRDD<String> result = playerEventsTuple.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, String>()
		{
			private static final long serialVersionUID = 8801411579412186819L;

			@Override
			public Iterable<String> call(Iterator<Tuple2<String, Integer>> tuples) throws Exception
			{
				ObjectMapper mapper = new ObjectMapper();
				mapper.registerModule(new JodaModule());
				
				Collection<String> result = new ArrayList<String>();
				
				while (tuples.hasNext())
				{
					Tuple2<String, Integer> t = tuples.next();
					SimpleModel ec = new SimpleModel();
					ec.eventName = "PlayerEventsCounter";
					ec.setOriginator("VimondAnalytics");
					ec.setTenant("tv2");
					ec.setRandomGuid();
					ec.setTimestamp(timestamp);
					ec.setGenericValue("timewindow", timewindow);
					ec.setGenericValue("data.playerEventType", t._1());
					ec.setGenericValue("counter", t._2());
					result.add(mapper.writeValueAsString(ec));
				}
				return result;
			}
		});
		
		JavaEsSpark.saveToEs(result, "vimond-batch/batch-playerEventsType");
		
		
	}
}
