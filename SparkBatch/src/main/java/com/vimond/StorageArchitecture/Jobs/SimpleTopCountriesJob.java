package com.vimond.StorageArchitecture.Jobs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.vimond.StorageArchitecture.Model.Event;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.joda.time.DateTime;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.StorageArchitecture.Model.SimpleModel;

public class SimpleTopCountriesJob extends WorkingJob
{

	private static final long serialVersionUID = -4447507237939137336L;
	
	public SimpleTopCountriesJob(JavaRDD<Event> inputDataset, DateTime timestamp, String timewindow)
	{
		super(inputDataset, timestamp, timewindow);
	}

	@Override
	public void run(JavaSparkContext ctx)
	{
		JavaPairRDD<String, String> pair_rdd_cn_ip = this.inputDataset.mapToPair(e -> new Tuple2<String, String>(e.getCountryName(), e.getIpAddress()));

	//	pair_rdd_cn_ip = pair_rdd_cn_ip.distinct();

		JavaPairRDD<String, Integer> mapped_rdd_cn = pair_rdd_cn_ip.mapToPair(t -> new Tuple2<String, Integer>(t._1(), 1));

		mapped_rdd_cn = mapped_rdd_cn.reduceByKey((x, y) -> x + y);

		JavaRDD<String> models_cn = mapped_rdd_cn.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, String>()
		{
			private static final long serialVersionUID = 716052106872985771L;

			public Iterable<String> call(Iterator<Tuple2<String, Integer>> tuples) throws Exception
			{
				ObjectMapper mapper = new ObjectMapper();
				mapper.registerModule(new JodaModule());
				List<String> ranking = new ArrayList<String>();
				while (tuples.hasNext())
				{
					Tuple2<String, Integer> t = tuples.next();
					SimpleModel tm = new SimpleModel();
					tm.eventName = "TopRanking";
					tm.genericValues.put("data.geo.country", t._1());
					tm.genericValues.put("counter", t._2());
					tm.setOriginator("VimondAnalytics");
					tm.setRandomGuid();
					tm.setTimestamp(timestamp);
					tm.setGenericValue("timewindow", timewindow);
					ranking.add(mapper.writeValueAsString(tm));
				}
				return ranking;
			}
		});

		JavaEsSpark.saveJsonToEs(models_cn, "vimond-batch/batch-topRanking");
	}
}
