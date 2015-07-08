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
import org.apache.spark.broadcast.Broadcast;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class SimpleTopCountriesJob extends WorkingJob
{

	private static final long serialVersionUID = -4447507237939137336L;
	
	public SimpleTopCountriesJob(JavaRDD<Event> rdd, Date minDate, Date maxDate)
	{
		super(rdd, minDate, maxDate);
	}

	@Override
	public void run(JavaSparkContext ctx)
	{
		JavaPairRDD<String, String> pair_rdd_cn_ip = this.inputDataset.mapToPair(e -> new Tuple2<String, String>(e.getCountryName(), e.getIpAddress()));

		pair_rdd_cn_ip = pair_rdd_cn_ip.distinct();

		JavaPairRDD<String, Integer> mapped_rdd_cn = pair_rdd_cn_ip.mapToPair(t -> new Tuple2<String, Integer>(t._1(), 1));

		mapped_rdd_cn = mapped_rdd_cn.reduceByKey((x, y) -> x + y);

		Broadcast<Date> maxDateBroad = ctx.broadcast(this.maxDate);
		Broadcast<Date> minDateBroad = ctx.broadcast(this.minDate);

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
					SimpleModel tm = new SimpleModel();
					tm.eventName = "TopRanking";
					tm.genericValues.put("data.country", t._1());
					tm.genericValues.put("data.counter", t._2());
					tm.setRandomGuid();
					tm.genericValues.put("timestamp", new Date());
					tm.genericValues.put("maxDate", maxDateBroad.getValue());
					tm.genericValues.put("minDate", minDateBroad.getValue());
					ranking.add(mapper.writeValueAsString(tm));
				}
				return ranking;
			}
		});

		JavaEsSpark.saveJsonToEs(models_cn, "spark/TopRanking");
	}
}
