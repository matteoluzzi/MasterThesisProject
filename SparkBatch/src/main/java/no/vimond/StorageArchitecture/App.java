package no.vimond.StorageArchitecture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import no.vimond.StorageArchitecture.Jobs.Job;
import no.vimond.StorageArchitecture.Jobs.JobName;
import no.vimond.StorageArchitecture.Jobs.JobsFactory;
import no.vimond.StorageArchitecture.Model.TestModel;
import no.vimond.StorageArchitecture.Utils.AppProperties;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Utils.StormEvent;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class App
{

	public static void main(String[] args)
	{

		AppProperties props = new AppProperties();

		final String appName = (String) props.get(Constants.APP_NAME_KEY);
		final int poolSize = Integer.parseInt(props.getProperty(Constants.POOL_SIZE_KEY));

		String master = System.getenv("MASTER");
		if (master == null)
		{
			master = "local";
		}

		// Spark settings

		SparkConf cfg = new SparkConf();
		cfg.setAppName(appName);
		cfg.setMaster(master);
		cfg.set(Constants.ES_INDEX_AUTO_CREATE_KEY, "true");

		// Complex classes must be (de)serialized with Kyro otherwise it won't
		// work
		Class[] serClasses = { StormEvent.class, TestModel.class };
		cfg.registerKryoClasses(serClasses);

		List<Future> jobs = new ArrayList<Future>();

		JavaSparkContext ctx = new JavaSparkContext(cfg);

		ExecutorService executorPool = Executors.newFixedThreadPool(poolSize);

		Future job_one = executorPool.submit(new Runnable()
		{

			@Override
			public void run()
			{
				Job job_one = JobsFactory.getFactory().createJob(JobName.SIMPLE, ctx, props);
				job_one.run();
			}

		});

		jobs.add(job_one);

		try
		{
			for (Future f : jobs)
				f.get();
		} catch (InterruptedException | ExecutionException e)
		{
			e.printStackTrace();
		} finally
		{
			executorPool.shutdown();
			ctx.stop();
		}
	}
}
