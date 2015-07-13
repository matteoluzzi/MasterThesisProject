package no.vimond.StorageArchitecture;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import no.vimond.StorageArchitecture.Jobs.Job;
import no.vimond.StorageArchitecture.Jobs.JobName;
import no.vimond.StorageArchitecture.Jobs.JobsFactory;
import no.vimond.StorageArchitecture.Jobs.LoadDataJob;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Utils.Event;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.pail.Pail;

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

	private String currentFolder;

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
		this.currentFolder = prop.getProperty("startingFolder");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int startJobs()
	{

		boolean ready = false;

		try
		{
			while (!ready)
			{
				Pail<String> pail = Pail.create(this.currentFolder, false);

				if (pail.exists("SUCCESS"))
				{
					ready = true;
				} else
					Thread.sleep(30000);
			}

		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}

		// start loadData job before starting the other

		LoadDataJob<Event> loadDataJob = (LoadDataJob<Event>) JobsFactory.getFactory().createJob(JobName.SIMPLE_DATA_LOADER, this.prop, null);

		loadDataJob.run(this.ctx);

		JavaRDD<Event> data_rdd = (JavaRDD<Event>) loadDataJob.getLoadedRDD();

		data_rdd.cache();

		// submit worker jobs to the executor

		this.submittedJobs.add(this.submitJob(JobName.SIMPLE_CONTENT_LOCATION, data_rdd));
		this.submittedJobs.add(this.submitJob(JobName.SIMPLE_TOP_APP, data_rdd));
		this.submittedJobs.add(this.submitJob(JobName.SIMPLE_TOP_ASSETS, data_rdd));
		this.submittedJobs.add(this.submitJob(JobName.SIMPLE_TOP_COUNTRIES, data_rdd));

		// close the executor
		this.pool.shutdown();

		for (Future f : this.submittedJobs)
		{
			try
			{
				f.get();
			} catch (InterruptedException | ExecutionException e)
			{
				LOG.error("Error while executing a job");
				return -1;
			}
		}

		return 0;
	}

	@SuppressWarnings("rawtypes")
	private Future submitJob(JobName name, JavaRDD<Event> data_rdd)
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
