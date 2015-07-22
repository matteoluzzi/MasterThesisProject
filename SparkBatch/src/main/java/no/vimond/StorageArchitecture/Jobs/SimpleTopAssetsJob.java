package no.vimond.StorageArchitecture.Jobs;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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

public class SimpleTopAssetsJob extends WorkingJob
{
	private static final long serialVersionUID = -8137849520886654258L;
	
	public SimpleTopAssetsJob(JavaRDD<Event> rdd)
	{
		super(rdd);
	}

	@Override
	public void run(JavaSparkContext ctx)
	{
		JavaPairRDD<Integer, String> pair_rdd_aid_ip = this.inputDataset.mapToPair(e -> new Tuple2<Integer, String>(e.getAssetId(), e.getIpAddress()));
		
		pair_rdd_aid_ip = pair_rdd_aid_ip.distinct();
		JavaPairRDD<Integer, Integer> mapped_rdd_aid = pair_rdd_aid_ip.mapToPair(t -> new Tuple2<Integer, Integer>(t._1(), 1));
		mapped_rdd_aid = mapped_rdd_aid.reduceByKey((x, y) -> x + y);
		
		JavaRDD<String> models_aid = mapped_rdd_aid.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Integer>>, String>()
				{
					private static final long serialVersionUID = 716052106872985771L;

					public Iterable<String> call(Iterator<Tuple2<Integer, Integer>> tuples) throws Exception
					{
						ObjectMapper mapper = new ObjectMapper();
						mapper.registerModule(new JodaModule());
						List<String> ranking = new ArrayList<String>();
						while (tuples.hasNext())
						{
							Tuple2<Integer, Integer> t = tuples.next();
							SimpleModel tm = new SimpleModel();
							tm.eventName = "TopAsset";
							tm.genericValues.put("data.assetId", t._1());
							tm.genericValues.put("data.counter", t._2());
							tm.setRandomGuid();
							tm.genericValues.put("timestamp", new Date());
							ranking.add(mapper.writeValueAsString(tm));
						}
						return ranking;
					}
				});
		
		JavaEsSpark.saveJsonToEs(models_aid, "spark/TopAssetId");
		
	}
}
