package no.vimond.StorageArchitecture.Jobs;

import java.util.Properties;

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

	public Job createJob(JobName job_name, JavaSparkContext ctx, Properties props)
	{
		switch (job_name)
		{
		case SIMPLE:
			return new SimpleTopCountriesJob(ctx, props);
		default:
			return null;
		}
	}

}
