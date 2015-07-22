package no.vimond.StorageArchitecture.Jobs;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import no.vimond.StorageArchitecture.Model.SimpleModel;
import no.vimond.StorageArchitecture.Utils.Event;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

public class ContentLocalizationJob extends WorkingJob
{
	private static final long serialVersionUID = -3085596859613929694L;

	public ContentLocalizationJob(JavaRDD<Event> inputDataset)
	{
		super(inputDataset);
	}

	@Override
	public void run(JavaSparkContext ctx)
	{
		JavaPairRDD<Tuple2<Integer, String>, String> pair_rdd = this.inputDataset.mapToPair(e -> new Tuple2<Tuple2<Integer, String>, String>(new Tuple2<Integer, String>(e.getAssetId(), e.getCountryName()), e.getIpAddress()));

		pair_rdd = pair_rdd.distinct();

		JavaPairRDD<Tuple2<Integer, String>, Integer> new_pair_rdd = pair_rdd.mapToPair(e -> new Tuple2<Tuple2<Integer, String>, Integer>(e._1(), 1));

		new_pair_rdd = new_pair_rdd.reduceByKey((x, y) -> x + y);

		JavaPairRDD<Integer, Tuple2<String, Integer>> asset_country_pair = new_pair_rdd.mapToPair(e -> new Tuple2<Integer, Tuple2<String, Integer>>(e._1()._1(), new Tuple2<String, Integer>(e._1()._2(), e._2())));

		JavaPairRDD<Integer, Iterable<Tuple2<String, Integer>>> final_rdd = asset_country_pair.groupByKey();

		JavaRDD<String> models = final_rdd.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Iterable<Tuple2<String, Integer>>>>, String>()
		{
			private static final long serialVersionUID = -4820806287198754944L;

			@Override
			public Iterable<String> call(Iterator<Tuple2<Integer, Iterable<Tuple2<String, Integer>>>> tuples) throws Exception
			{
				ObjectMapper mapper = new ObjectMapper();
				mapper.registerModule(new JodaModule());
				List<String> result = new ArrayList<String>();
				while(tuples.hasNext())
				{
					Tuple2<Integer, Iterable<Tuple2<String, Integer>>> t = tuples.next();
					
					SimpleModel tm = new SimpleModel();
					tm.eventName = "ContentLocation";
					tm.setGenericValue("data.assetId", t._1());
					Iterable<Tuple2<String, Integer>> countriesList = t._2();
					List<Map<String, Object>> ranking = new ArrayList<Map<String, Object>>();
					
					for(Tuple2<String, Integer> country_tuple : countriesList)
					{
						Map<String, Object> element = new HashMap<String, Object>();
						element.put("country", country_tuple._1());
						element.put("count", country_tuple._2());
						ranking.add(element);
					}
					tm.setGenericValue("data.ranking", ranking);
					tm.setRandomGuid();
					tm.setGenericValue("timestamp", new Date());
					
					result.add(mapper.writeValueAsString(tm));
				}
				return result;
			}
		});
		
		JavaEsSpark.saveJsonToEs(models, "spark/ContentLocation");

	}
}
