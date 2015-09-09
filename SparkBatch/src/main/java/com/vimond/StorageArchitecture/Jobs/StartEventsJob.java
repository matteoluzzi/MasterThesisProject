package com.vimond.StorageArchitecture.Jobs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.joda.time.DateTime;

import com.vimond.StorageArchitecture.Processing.ExtractUserAgent;
import com.vimond.StorageArchitecture.Utils.EventInfo;
import com.vimond.utils.data.SparkEvent;

/**
 * Simple job for filtering on the event of a player start. It calculates also the browser and os versions from user agent. Its result is then used by other jobs.
 * @author matteoremoluzzi
 *
 */
public class StartEventsJob extends WorkingJob<SparkEvent>
{	
	private static final long serialVersionUID = 1564561353377085870L;
	
	private JavaRDD<EventInfo> filteredDataset;
	
	public StartEventsJob(JavaRDD<SparkEvent> inputDataset, DateTime timestamp, String timewindow)
	{
		super(inputDataset, timestamp, timewindow);
	}

	@Override
	public void run(JavaSparkContext ctx)
	{
		JavaRDD<SparkEvent> startEventsRdd = this.inputDataset.filter(new Function<SparkEvent, Boolean>()
		{
			private static final long serialVersionUID = -3788210390379228766L;

			@Override
			public Boolean call(SparkEvent event) throws Exception
			{
				String playerEvent = event.getPlayerEvent();
				if(playerEvent != null && playerEvent.equals("str-start"))
					return true;
				else return false;
			}
		});
		
		this.filteredDataset = startEventsRdd.mapPartitions(new ExtractUserAgent());
	}
	
	public JavaRDD<EventInfo> getFilteredRDD()
	{
		if(this.filteredDataset != null)
			return this.filteredDataset;
		else
			return null;
	}

}
