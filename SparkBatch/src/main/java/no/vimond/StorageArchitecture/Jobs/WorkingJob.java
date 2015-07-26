package no.vimond.StorageArchitecture.Jobs;

import no.vimond.StorageArchitecture.Model.Event;

import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;

/**
 * Abstract class for jobs performing operations from an existing RDD
 * @author matteoremoluzzi
 *
 */
public abstract class WorkingJob implements Job
{
	
	protected JavaRDD<Event> inputDataset;
	protected DateTime timestamp;
	protected String timewindow;
	
	private static final long serialVersionUID = -1966157014649117505L;
	
	public WorkingJob(JavaRDD<Event> inputDataset, DateTime timestamp, String timewindow)
	{
		this.inputDataset = inputDataset;
		this.timestamp = timestamp;
		this.timewindow = timewindow;
	}
}
