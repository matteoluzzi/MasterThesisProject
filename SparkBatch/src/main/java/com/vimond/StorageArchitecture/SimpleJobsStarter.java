package com.vimond.StorageArchitecture;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vimond.StorageArchitecture.Jobs.Job;
import com.vimond.StorageArchitecture.Jobs.JobName;
import com.vimond.StorageArchitecture.Jobs.JobsFactory;
import com.vimond.StorageArchitecture.Jobs.LoadDataJob;
import com.vimond.StorageArchitecture.Jobs.StartEventsJob;
import com.vimond.StorageArchitecture.Utils.EventInfo;
import com.vimond.utils.data.Constants;
import com.vimond.utils.data.SparkEvent;

/**
 * Class in charge to start at the same time some simple jobs working on the
 * same rdd. It first loads the rdd from files and then submits in parallel jobs
 * to an executor
 * 
 * @author matteoremoluzzi
 *
 */
public class SimpleJobsStarter implements Serializable
{

	private static final long serialVersionUID = 1L;
	private Logger LOG = LoggerFactory.getLogger(SimpleJobsStarter.class);

	private JavaSparkContext ctx;
	private Properties prop;
	private ExecutorService pool;
	private List<Future> submittedJobs;
	
	public SimpleJobsStarter(JavaSparkContext ctx, Properties prop)
	{
		this.ctx = ctx;
		this.prop = prop;
		this.submittedJobs = new ArrayList<Future>();

		
		final int poolSize = Integer.parseInt(this.prop.getProperty(Constants.POOL_SIZE_KEY));
		this.pool = Executors.newFixedThreadPool(poolSize);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int startJobs()
	{
		
		// start loadData job before starting the other

		LoadDataJob<SparkEvent> loadDataJob = (LoadDataJob<SparkEvent>) JobsFactory.getFactory().createJob(JobName.SIMPLE_DATA_LOADER, this.prop, null);

		loadDataJob.run(this.ctx);

		JavaRDD<SparkEvent> data_rdd = (JavaRDD<SparkEvent>) loadDataJob.getLoadedRDD();
		
		//keep in memory the input dataset for better performances
		data_rdd.cache();
		
		// submit worker jobs to the executor

		//submit one-step jobs
		this.submittedJobs.add(this.submitJob(JobName.COUNTER_EVENT_TYPE, data_rdd));
		this.submittedJobs.add(this.submitJob(JobName.COUNTER_END_BY_ASSET, data_rdd));
		
		StartEventsJob startEventJob = (StartEventsJob) JobsFactory.getFactory().createJob(JobName.START_EVENTS, this.prop, data_rdd);
		startEventJob.run(this.ctx);
		
		JavaRDD<EventInfo> start_events_rdd = startEventJob.getFilteredRDD();
		
		this.submittedJobs.add(this.submitJob(JobName.COUNTER_START_BY_ASSET, start_events_rdd));
		this.submittedJobs.add(this.submitJob(JobName.TOP_BROWSER, start_events_rdd));
		this.submittedJobs.add(this.submitJob(JobName.TOP_OS, start_events_rdd));
		this.submittedJobs.add(this.submitJob(JobName.TOP_VIDEOFORMAT, start_events_rdd));
		

		// close the executor
		this.pool.shutdown();

		for (Future f : this.submittedJobs)
		{
			try
			{
				f.get();
			} catch (InterruptedException | ExecutionException e)
			{
				LOG.error("Error while executing a job: {}", e);
				return -1;
			}
		}

		//remove data from memory after completing the jobs
		data_rdd.unpersist();
		
		return 0;
	}

	@SuppressWarnings("rawtypes")
	private Future submitJob(JobName name, JavaRDD data_rdd)
	{
		return this.pool.submit(new Runnable()
		{
			@Override
			public void run()
			{
				Job job = JobsFactory.getFactory().createJob(name, prop, data_rdd);
				job.run(ctx);
			}
		});
	}
}
