package no.vimond.StorageArchitecture.Jobs;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import no.vimond.StorageArchitecture.Model.TestModel;
import no.vimond.StorageArchitecture.Processing.ExtractGeoIPInfo;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Utils.DataLoader;
import no.vimond.StorageArchitecture.Utils.StormEvent;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class SimpleTopCountriesJob implements Job
{

	private static final long serialVersionUID = -4447507237939137336L;

	private static JavaSparkContext ctx;
	private final Properties props;
	private static ObjectMapper mapper = ObjectMapperConfiguration.configure();

	public SimpleTopCountriesJob(JavaSparkContext ctx, Properties props)
	{
		this.ctx = ctx;
		this.props = props;
	}

	@Override
	public void run()
	{
		DataLoader dataLoader = new DataLoader();
		JavaRDD<StormEvent> rdd = dataLoader.loadEventsFromFiles(ctx);
		rdd.cache();

		final boolean dbLiteVersion = Boolean.parseBoolean(props.getProperty(Constants.DB_LITE_KEY));

		// broadcast dblite value so it can be used by every executor
		Broadcast<Boolean> dbLite = ctx.broadcast(dbLiteVersion);

		rdd = rdd.map(new ExtractGeoIPInfo(dbLite));

		JavaPairRDD<String, String> pair_rdd_cn_ip = rdd.mapToPair(e -> new Tuple2<String, String>(e.getCountryName(), e.getIpAddress()));
		JavaPairRDD<Integer, String> pair_rdd_aid_ip = rdd.mapToPair(e -> new Tuple2<Integer, String>(e.getAssetId(), e.getIpAddress()));
		
		
		pair_rdd_cn_ip = pair_rdd_cn_ip.distinct();
		pair_rdd_aid_ip = pair_rdd_aid_ip.distinct();

		JavaPairRDD<String, Integer> mapped_rdd_cn = pair_rdd_cn_ip.mapToPair(t -> new Tuple2<String, Integer>(t._1(), 1));
		
		JavaPairRDD<Integer, Integer> mapped_rdd_aid = pair_rdd_aid_ip.mapToPair(t -> new Tuple2<Integer, Integer>(t._1(), 1));
			
		mapped_rdd_cn = mapped_rdd_cn.reduceByKey((x, y) -> x + y);
		
		mapped_rdd_aid = mapped_rdd_aid.reduceByKey((x, y) -> x + y);
		
		JavaRDD<String> models_cn = mapped_rdd_cn.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, String>()
		{
			private static final long serialVersionUID = 716052106872985771L;

			public Iterable<String> call(Iterator<Tuple2<String, Integer>> tuples) throws Exception
			{
				ObjectMapper mapper = ObjectMapperConfiguration.configure();
				List<String> ranking = new ArrayList<String>();
				while (tuples.hasNext())
				{
					Tuple2<String, Integer> t = tuples.next();
					TestModel tm = new TestModel();
					tm.eventName = "TopRanking";
					tm.genericValues.put("data.country", t._1());
					tm.genericValues.put("data.counter", t._2());
					tm.setRandomGuid();
					tm.genericValues.put("timestamp", new Date());
					ranking.add(mapper.writeValueAsString(tm));
				}
				return ranking;
			}
		});
		
		JavaRDD<String> models_aid = mapped_rdd_aid.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Integer>>, String>()
				{
					private static final long serialVersionUID = 716052106872985771L;

					public Iterable<String> call(Iterator<Tuple2<Integer, Integer>> tuples) throws Exception
					{
						ObjectMapper mapper = ObjectMapperConfiguration.configure();
						List<String> ranking = new ArrayList<String>();
						while (tuples.hasNext())
						{
							Tuple2<Integer, Integer> t = tuples.next();
							TestModel tm = new TestModel();
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
		
		JavaEsSpark.saveJsonToEs(models_cn, "spark/TopRanking");
		JavaEsSpark.saveJsonToEs(models_aid, "spark/TopAssetId");

		// pair_rdd.saveAsTextFile("simple_job");

	}
}
