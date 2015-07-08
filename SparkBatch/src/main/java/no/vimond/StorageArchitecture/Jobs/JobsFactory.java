package no.vimond.StorageArchitecture.Jobs;

import java.util.Date;
import java.util.Properties;

import no.vimond.StorageArchitecture.Utils.Event;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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

	public Job createJob(JobName job_name, JavaSparkContext ctx, Properties props, JavaRDD<Event> rdd, Date minDate, Date maxDate)
	{
		switch (job_name)
		{
		case SIMPLE_TOP_COUNTRIES:
			return new SimpleTopCountriesJob(rdd, minDate, maxDate);
		case SIMPLE_TOP_ASSETS:
			return new SimpleTopAssetsJob(rdd, minDate, maxDate);
		case SIMPLE_CONTENT_LOCATION:
			return new ContentLocalizationJob(rdd, minDate, maxDate);
		default:
			return new LoadDataJob<Event>(props, Event.class);
		}
	}

}
