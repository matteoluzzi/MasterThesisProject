package no.vimond.StorageArchitecture.Jobs;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Basic interface for job object
 * @author matteoremoluzzi
 *
 */
public interface Job extends Serializable
{
	/**
	 * Standard implementation for job action. Each subclass must implement it with a custom behavior
	 */
	public void run(JavaSparkContext ctx);
}
