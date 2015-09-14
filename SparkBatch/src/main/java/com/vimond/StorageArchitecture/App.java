package com.vimond.StorageArchitecture;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.vimond.StorageArchitecture.HDFS.DataPoller;
import com.vimond.StorageArchitecture.Utils.Utility;
import com.vimond.utils.config.AppProperties;

public class App
{
	private static Logger LOG = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws IOException, InterruptedException
	{

		if (args.length != 2)
		{
			LOG.error("Missing arguments, exiting now!");
			System.exit(-1);
		}

		AppProperties props = null;
		String path = null;
		String freq = null;
		try
		{
			path = args[0];
			freq = args[1];

			LOG.info("Folder: " + path);
			LOG.info("Frequency: " + freq);

			props = new AppProperties();

			// path and freq properties are passed dynamically at each execution
			// of the program
			props.addOrUpdateProperty("path", path);
			props.addOrUpdateProperty("freq", freq);
		} catch (Exception e)
		{
			LOG.error("Error while parsing input arguments: {}, {}", e.getClass(), e.getMessage());
			System.exit(-1);
		}
		
		//initialize the context
		JavaSparkContext ctx = App.initializeSparkContext(props);

		DataPoller dataInit = new DataPoller(props);

		if (dataInit.getMasterPail() != null)
		{
			DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/HH/mm").withZone(DateTimeZone.forID("Europe/Oslo"));

			String folder_path = Utility.extractDate(path);
			props.addOrUpdateProperty("timestamp", formatter.parseDateTime(folder_path));
			props.addOrUpdateProperty("timewindow", freq);
			

			//String dataPath = dataInit.ingestNewData();

			String dataPath = path;

			if (dataPath != null)
				props.addOrUpdateProperty("dataPath", dataPath);
			else
			{
				LOG.error("Error while moving data into the snapshot directory");
				System.exit(0);
			}

			JobsStarter starter = new JobsStarter(ctx, props);
			starter.startJobs();
		}

		else
		{
			LOG.error("Data not aligned with batch program");
			System.exit(0);
		}
	}

	/**
	 * Initialize a SparkContext object for the application according to the
	 * properties specified in the properties file
	 * 
	 * @param props
	 * @return an initialized JavaSparkContext
	 */
	public static JavaSparkContext initializeSparkContext(AppProperties props)
	{
		
		final String appName = "SparkBatch";

		// Spark settings
		SparkConf cfg = new SparkConf();
		cfg.setAppName(appName);
		/**
		 * Properties file are passed through a configuration file, see oozie spark for details
		 */
		
//		cfg.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		cfg.set("spark.kyro.registrator", "com.vimond.StorageArchitecture.Utils.ClassRegistrator");
//		cfg.set("spark.scheduler.allocation.file", "/var/files/batch/poolScheduler.xml");
		
//		// ES settings
//		cfg.set("es.index.auto.create", "true");
//		cfg.set("es.nodes", (String) props.getOrDefault("es.nodes", "localhost"));
//		cfg.set("es.input.json", "true");
		
		for(Tuple2<String, String> prop : cfg.getAll())
			LOG.info(prop._1() + " = " + prop._2());
		
		return new JavaSparkContext(cfg);

	}

}
