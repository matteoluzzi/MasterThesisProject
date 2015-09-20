package com.vimond.StorageArchitecture.Jobs;

import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;

import com.vimond.utils.data.SparkEvent;

public class JobsFactory
{
	private static JobsFactory instance = new JobsFactory();

	private JobsFactory()
	{
	}

	public static JobsFactory getFactory()
	{
		return instance;
	}

	@SuppressWarnings({"unchecked" })
	public Job createJob(JobName job_name, Properties props, JavaRDD rdd)
	{
		DateTime timestamp = (DateTime) props.get("timestamp");
		String timewindow = props.getProperty("timewindow");
		
		switch (job_name)
		{
		
		case SIMPLE_DATA_LOADER:
			return new LoadDataJob<SparkEvent>(props, SparkEvent.class);
		case COUNTER_END_BY_ASSET:
			return new EndEventsPerAssetJob(rdd, timestamp, timewindow);
		case COUNTER_EVENT_TYPE:
			return new PlayerEventTypeCounter(rdd, timestamp, timewindow);
		case COUNTER_START_BY_ASSET:
			return new StartEventsPerAssetJob(rdd, timestamp, timewindow);
		case TOP_BROWSER:
			return new TopBrowserJob(rdd, timestamp, timewindow);
		case TOP_OS:
			return new TopOsJob(rdd, timestamp, timewindow);
		case TOP_VIDEOFORMAT:
			return new TopVideoFormatJob(rdd, timestamp, timewindow);
		case START_EVENTS:
			return new StartEventsJob(rdd, timestamp, timewindow);
		}
		return null;
	}

}
