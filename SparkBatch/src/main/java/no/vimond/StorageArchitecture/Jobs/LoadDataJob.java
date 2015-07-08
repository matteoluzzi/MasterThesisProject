package no.vimond.StorageArchitecture.Jobs;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import no.vimond.StorageArchitecture.Processing.ExtractGeoIPInfo;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Utils.SparkAccumulator.MaxDateAccumulatorParam;
import no.vimond.StorageArchitecture.Utils.SparkAccumulator.MinDateAccumulatorParam;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.shared.ObjectMapperConfiguration;

/**
 * Generic job for loading events from text files. It contains information about the timeframe of the data
 * @author matteoremoluzzi
 *
 * @param <T> subclass of <code>VimondEventAny</class>
 */
public class LoadDataJob<T extends VimondEventAny> implements Job
{
	private static final long serialVersionUID = 4238953880867095830L;
	
	protected Date minDate;
	protected Date maxDate;
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
		
		
		JavaRDD<String> string_data = ctx.textFile(props.getProperty(Constants.INPUT_PATH_KEY));
		
		Accumulator<Date> maxDate = ctx.accumulator(new Date(0), new MaxDateAccumulatorParam()); 
		Accumulator<Date> minDate = ctx.accumulator(new Date(0), new MinDateAccumulatorParam()); 
		
		this.inputDataset = string_data.mapPartitions(new FlatMapFunction<Iterator<String>, T>()
		{
			private static final long serialVersionUID = -5334810881750555414L;

			@Override
			public Iterable<T> call(Iterator<String> str_events) throws Exception
			{
				ObjectMapper mapper = ObjectMapperConfiguration.configure();
				ArrayList<T> events = new ArrayList<T>();
				while(str_events.hasNext())
				{
					String next = str_events.next();
					T event = mapper.readValue(next, clazz);
					maxDate.add(event.getTimestamp().toDate());
					minDate.add(event.getTimestamp().toDate());
					events.add(event);
				}
				return events;
			}
		});
		
		this.minDate = minDate.value();
		this.maxDate = maxDate.value();
		
		inputDataset.collect();
		
		System.out.println(minDate.value());
		
	//	this.inputDataset = inputDataset.map((Function<T, T>) new ExtractGeoIPInfo(dbLite));
	}
	
	public JavaRDD<? extends VimondEventAny> getLoadedRDD()
	{
		return this.inputDataset;
	}
	
	public Date getBeginDate()
	{
		return this.minDate;
	}
	
	public Date getEndingDate()
	{
		return this.maxDate;
	}
}