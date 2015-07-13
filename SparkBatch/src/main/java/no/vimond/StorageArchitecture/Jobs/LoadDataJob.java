package no.vimond.StorageArchitecture.Jobs;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import no.vimond.StorageArchitecture.Processing.ExtractGeoIPInfo;
import no.vimond.StorageArchitecture.Utils.Constants;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;

import com.backtype.hadoop.pail.Pail;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.shared.ObjectMapperConfiguration;

/**
 * Generic job for loading events from text files. It contains information about
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
		JavaPairRDD<String, PortableDataStream> string_data = ctx.binaryFiles(props.getProperty(Constants.INPUT_PATH_KEY), 3);
		
		
		this.inputDataset = string_data.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,PortableDataStream>>, T>()
		{

			@Override
			public Iterable<T> call(Iterator<Tuple2<String, PortableDataStream>> t) throws Exception
			{
				ObjectMapper mapper = ObjectMapperConfiguration.configure();
				ArrayList<T> events = new ArrayList<T>();
				while(t.hasNext())
				{
					PortableDataStream stream = t.next()._2();
				    DataInputStream input_stream =stream.open();
				    String jsonEvent;
				    T event = null;
				    while((jsonEvent = input_stream.readLine()) != null)
				    {
				    	if(jsonEvent.startsWith("{"))
				    		event = mapper.readValue(jsonEvent, clazz);
				    	else
				    	{
				    		int x =jsonEvent.indexOf("{");
				    		if(x != -1)
				    			event = mapper.readValue(jsonEvent.substring(x), clazz);
				    	}
				    	if(event != null)
				    		events.add(event);
				    }
				}
				return events;
			}
		});
	/*	
		
		this.inputDataset = string_data.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, String>>, T>()
		{
			private static final long serialVersionUID = -5334810881750555414L;

			@Override
			public Iterable<T> call(Iterator<Tuple2<String, String>> str_events) throws Exception
			{
				ObjectMapper mapper = ObjectMapperConfiguration.configure();
				ArrayList<T> events = new ArrayList<T>();
				while(str_events.hasNext())
				{
					Tuple2<String, String> next = str_events.next();
					System.out.println(next._1());
					System.out.println(next._2());
				//	T event = mapper.readValue(next, clazz);
				//	events.add(event);
				}
				return events;
			}
		});
		*/
		this.inputDataset = inputDataset.map((Function<T, T>) new ExtractGeoIPInfo(dbLite));
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