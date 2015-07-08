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

	public Job createJob(JobName job_name, JavaSparkContext ctx, Properties props, JavaRDD<Event> rdd)
	{
		switch (job_name)
		{
		case SIMPLE_TOP_COUNTRIES:
			return new SimpleTopCountriesJob(rdd);
		case SIMPLE_TOP_ASSETS:
			return new SimpleTopAssetsJob(rdd);
		case SIMPLE_CONTENT_LOCATION:
			return new ContentLocalizationJob(rdd);
		case SIMPLE_TOP_APP:
			return new SimpleTopAppJob(rdd);
		default:
			return new LoadDataJob<Event>(props, Event.class);
		}
	}

}
