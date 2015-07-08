package no.vimond.StorageArchitecture.Jobs;

import java.util.Date;

import no.vimond.StorageArchitecture.Utils.Event;

import org.apache.spark.api.java.JavaRDD;

/**
 * Abstract class for jobs performing operations from an existing RDD
 * @author matteoremoluzzi
 *
 */
public abstract class WorkingJob implements Job
{
	
	protected JavaRDD<Event> inputDataset;
	
	private static final long serialVersionUID = -1966157014649117505L;
	
	public WorkingJob(JavaRDD<Event> inputDataset)
	{
		this.inputDataset = inputDataset;
	}
}
