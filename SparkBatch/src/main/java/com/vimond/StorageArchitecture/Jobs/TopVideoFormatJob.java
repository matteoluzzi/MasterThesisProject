package com.vimond.StorageArchitecture.Jobs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.joda.time.DateTime;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.StorageArchitecture.Utils.EventInfo;
import com.vimond.utils.data.SimpleModel;

public class TopVideoFormatJob extends WorkingJob<EventInfo>
{

	private static final long serialVersionUID = 4238311673383860862L;

	public TopVideoFormatJob(JavaRDD<EventInfo> inputDataset, DateTime timestamp, String timewindow)
	{
		super(inputDataset, timestamp, timewindow);
	}

	@Override
	public void run(JavaSparkContext ctx)
	{
		JavaPairRDD<String, Integer> mappedRdd = this.inputDataset.mapToPair(t -> new Tuple2<String, Integer>(t.getVideoFormat(), 1));

		mappedRdd = mappedRdd.reduceByKey((x, y) -> x + y);

		JavaRDD<String> result = mappedRdd.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, String>()
		{

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Iterator<Tuple2<String, Integer>> tuples) throws Exception
			{

				ObjectMapper mapper = new ObjectMapper();
				mapper.registerModule(new JodaModule());

				Collection<String> result = new ArrayList<String>();

				while (tuples.hasNext())
				{
					Tuple2<String, Integer> t = tuples.next();

					SimpleModel counterByAsset = new SimpleModel();
					counterByAsset.setOriginator("VimondAnalytics");
					counterByAsset.setTenant("tv2 Sumo");
					counterByAsset.eventName = "TopVideoFormat";
					counterByAsset.setRandomGuid();
					counterByAsset.setTimestamp(timestamp);
					counterByAsset.setGenericValue("timewindow", timewindow);
					counterByAsset.setGenericValue("data.videoFormat", t._1());
					counterByAsset.setGenericValue("counter", t._2());

					result.add(mapper.writeValueAsString(counterByAsset));
				}

				return result;
			}
		});

		JavaEsSpark.saveToEs(result, "vimond-batch/batch-topVideoFormat");
	}

}

