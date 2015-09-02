package com.vimond.StorageArchitecture.Jobs;


import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;

/**
 * Abstract class for jobs performing operations from an existing RDD
 * @author matteoremoluzzi
 *
 */
public abstract class WorkingJob<T> implements Job
{
	
	protected JavaRDD<T> inputDataset;
	protected DateTime timestamp;
	protected String timewindow;
	
	private static final long serialVersionUID = -1966157014649117505L;
	
	public WorkingJob(JavaRDD<T> inputDataset, DateTime timestamp, String timewindow)
	{
		this.inputDataset = inputDataset;
		this.timestamp = timestamp;
		this.timewindow = timewindow;
	}
}
