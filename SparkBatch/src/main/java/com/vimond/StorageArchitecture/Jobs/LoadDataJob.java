package com.vimond.StorageArchitecture.Jobs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.StorageArchitecture.Processing.ExtractGeoIPInfo;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.utils.data.Constants;

/**
 * Generic job for loading events from an hdfs folder. It contains information about
 * the timeframe of the data
 * 
 * @author matteoremoluzzi
 *
 * @param <T>
 *            subclass of <code>VimondEventAny</class>
 */
public class LoadDataJob<T extends VimondEventAny> implements Job
{
	private static final long serialVersionUID = 4238953880867095830L;

	protected JavaRDD<T> inputDataset;
	protected Properties props;
	protected final Class<T> clazz;

	public LoadDataJob(Properties props, Class<T> clazz)
	{
		this.props = props;
		this.clazz = clazz;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run(JavaSparkContext ctx)
	{

		final boolean dbLiteVersion = Boolean.parseBoolean(props.getProperty(Constants.DB_LITE_KEY));

		// broadcast dblite value so it can be used by every executor
		Broadcast<Boolean> dbLite = ctx.broadcast(dbLiteVersion);

		//read from dataPath folder all the .pailfile available
		JavaPairRDD<BytesWritable,NullWritable> string_data = ctx.sequenceFile(this.props.getProperty("dataPath") + "/*.pailfile", BytesWritable.class, NullWritable.class);
		
		this.inputDataset = string_data.mapPartitions(new FlatMapFunction<Iterator<Tuple2<BytesWritable,NullWritable>>, T>()
		{

			private static final long serialVersionUID = -5334810881750555414L;

			@Override
			public Iterable<T> call(Iterator<Tuple2<BytesWritable, NullWritable>> t) throws Exception
			{
				ObjectMapper mapper = new ObjectMapper();
				mapper.registerModule(new JodaModule());
				ArrayList<T> events = new ArrayList<T>();
				while(t.hasNext())
				{
					Tuple2<BytesWritable, NullWritable> tuple = t.next();
					BytesWritable bw = tuple._1();
					String jsonObject = new String(bw.getBytes());
					T event = mapper.readValue(jsonObject, clazz);
					events.add(event);
				}
				return events;
			}
		});
		
		//calculate the distinct values over the dataset due to "at-least-once" message semantic of kafka.
		this.inputDataset = inputDataset.distinct();
				
	}

	public JavaRDD<? extends VimondEventAny> getLoadedRDD()
	{
		return this.inputDataset;
	}
}