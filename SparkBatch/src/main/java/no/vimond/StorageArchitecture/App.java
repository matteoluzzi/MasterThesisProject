package no.vimond.StorageArchitecture;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import no.vimond.StorageArchitecture.Jobs.Job;
import no.vimond.StorageArchitecture.Jobs.JobName;
import no.vimond.StorageArchitecture.Jobs.JobsFactory;
import no.vimond.StorageArchitecture.Jobs.LoadDataJob;
import no.vimond.StorageArchitecture.Model.SimpleModel;
import no.vimond.StorageArchitecture.Utils.AppProperties;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Utils.Event;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App
{

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args)
	{

		AppProperties props = new AppProperties();

		final String appName = (String) props.get(Constants.APP_NAME_KEY);
		final int poolSize = Integer.parseInt(props.getProperty(Constants.POOL_SIZE_KEY));

		final String master = "local";

		// Spark settings

		SparkConf cfg = new SparkConf();
		cfg.setAppName(appName);
		cfg.setMaster(master);
		cfg.set(Constants.ES_INDEX_AUTO_CREATE_KEY, "true");

		// Complex classes must be (de)serialized with Kyro otherwise it won't
		// work
		Class[] serClasses = { Event.class, SimpleModel.class };
		cfg.registerKryoClasses(serClasses);

		List<Future> jobs = new ArrayList<Future>();

		JavaSparkContext ctx = new JavaSparkContext(cfg);
		
		LoadDataJob<Event> loadDataJob = new LoadDataJob<Event>(props, Event.class);
		
		loadDataJob.run(ctx);
		
		JavaRDD<Event> inputDataset = (JavaRDD<Event>) loadDataJob.getLoadedRDD();
		
		//cache rdd!
		inputDataset.cache();
		
		Date minDate = loadDataJob.getBeginDate();
		Date maxDate = loadDataJob.getEndingDate();
		
		inputDataset.collect();
		System.out.println(minDate);
		
		ExecutorService executorPool = Executors.newFixedThreadPool(poolSize);
		
		Future job_one = executorPool.submit(new Runnable()
		{
			@Override
			public void run()
			{
				Job job_one = JobsFactory.getFactory().createJob(JobName.SIMPLE_TOP_COUNTRIES, ctx, props, inputDataset, minDate, maxDate);
				job_one.run(ctx);
			}
		});

		
	//jobs.add(job_one);

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
