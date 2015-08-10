package com.vimond.StorageArchitecture.Jobs;

import java.util.Properties;

import com.vimond.StorageArchitecture.Model.Event;

import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;

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

	public Job createJob(JobName job_name, Properties props, JavaRDD<Event> rdd)
	{
		DateTime timestamp = (DateTime) props.get("timestamp");
		String timewindow = props.getProperty("timewindow");
		
		switch (job_name)
		{
		case SIMPLE_TOP_COUNTRIES:
			return new SimpleTopCountriesJob(rdd, timestamp, timewindow);
		case SIMPLE_TOP_ASSETS:
			return new SimpleTopAssetsJob(rdd, timestamp, timewindow);
		case SIMPLE_CONTENT_LOCATION:
			return new ContentLocalizationJob(rdd, timestamp, timewindow);
		case SIMPLE_TOP_APP:
			return new SimpleTopAppJob(rdd, timestamp, timewindow);
		case SIMPLE_DATA_LOADER:
			return new LoadDataJob<Event>(props, Event.class);
		}
		return null;
	}

}
