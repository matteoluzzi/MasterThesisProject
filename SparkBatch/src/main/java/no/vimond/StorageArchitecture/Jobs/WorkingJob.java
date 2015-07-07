package no.vimond.StorageArchitecture.Jobs;

import java.util.Date;

import no.vimond.StorageArchitecture.Utils.StormEvent;

import org.apache.spark.api.java.JavaRDD;

/**
 * Abstract class for jobs performing operations from an existing RDD
 * @author matteoremoluzzi
 *
 */
public abstract class WorkingJob implements Job
{
	
	protected JavaRDD<StormEvent> inputDataset;
	protected Date minDate;
	protected Date maxDate;
	
	private static final long serialVersionUID = -1966157014649117505L;
	
	public WorkingJob(JavaRDD<StormEvent> inputDataset, Date minDate, Date maxDate)
	{
		this.minDate = minDate;
		this.maxDate = maxDate;
		this.inputDataset = inputDataset;
	}
}