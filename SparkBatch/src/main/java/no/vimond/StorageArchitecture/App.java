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

		final String master = "spark://Matteos-MBP.vimond.local:7077";
		

		// Spark settings

		SparkConf cfg = new SparkConf();
		cfg.setAppName(appName);
		cfg.setMaster(master);
		cfg.set(Constants.ES_INDEX_AUTO_CREATE_KEY, "true");
		cfg.set("es.nodes","localhost");
		cfg.set("es.port", "9200");
		cfg.set("es.input.json", "true");
		cfg.set("spark.executor.memory", "2g");
		

		// Complex classes must be (de)serialized with Kyro otherwise it won't
		// work
		Class[] serClasses = { Event.class, SimpleModel.class };
		cfg.registerKryoClasses(serClasses);

		List<Future> jobs = new ArrayList<Future>();

		JavaSparkContext ctx = new JavaSparkContext(cfg);

		LoadDataJob<Event> loadDataJob = new LoadDataJob<Event>(props, Event.class);

		loadDataJob.run(ctx);

		JavaRDD<Event> inputDataset = (JavaRDD<Event>) loadDataJob.getLoadedRDD();

		// cache rdd!
		inputDataset.cache();
		
		inputDataset.collect();

		ExecutorService executorPool = Executors.newFixedThreadPool(poolSize);

		Future job_one = executorPool.submit(new Runnable()
		{
			@Override
			public void run()
			{
				Job job_one = JobsFactory.getFactory().createJob(JobName.SIMPLE_TOP_COUNTRIES, ctx, props, inputDataset);
				job_one.run(ctx);
			}
		});

		Future job_two = executorPool.submit(new Runnable()
		{
			@Override
			public void run()
			{
				Job job_one = JobsFactory.getFactory().createJob(JobName.SIMPLE_CONTENT_LOCATION, ctx, props, inputDataset);
				job_one.run(ctx);
			}
		});

		Future job_three = executorPool.submit(new Runnable()
		{
			@Override
			public void run()
			{
				Job job_one = JobsFactory.getFactory().createJob(JobName.SIMPLE_TOP_ASSETS, ctx, props, inputDataset);
				job_one.run(ctx);
			}
		});

		Future job_four = executorPool.submit(new Runnable()
		{
			@Override
			public void run()
			{
				Job job_one = JobsFactory.getFactory().createJob(JobName.SIMPLE_TOP_APP, ctx, props, inputDataset);
				job_one.run(ctx);
			}
		});

		jobs.add(job_one);
		jobs.add(job_two);
		jobs.add(job_three);
		jobs.add(job_four);
		
		
		for(Future f : jobs)
			try
			{
				f.get();
			} catch (InterruptedException | ExecutionException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally
			{
				executorPool.shutdown();
				ctx.close();
			}

	}
	
}
