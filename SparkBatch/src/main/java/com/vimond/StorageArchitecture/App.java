package com.vimond.StorageArchitecture;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.vimond.utils.config.AppProperties;

public class App
{
	private static Logger LOG = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws IOException, InterruptedException
	{

		if (args.length != 3)
		{
			LOG.error("Missing arguments, exiting now!");
			System.exit(-1);
		}

		AppProperties props = null;
		String path = null;
		String freq = null;
		String es_address = null;

		try
		{
			path = args[0];
			freq = args[1];
			es_address = args[2];

			LOG.info("Folder: " + path);
			LOG.info("Frequency: " + freq);
			LOG.info("Es address: " + es_address);

			props = new AppProperties();

			// path and freq properties are passed dynamically at each execution
			// of the program
			props.addOrUpdateProperty("path", path);
			props.addOrUpdateProperty("freq", freq);
			props.addOrUpdateProperty("es.nodes", es_address);
			
		} catch (Exception e)
		{
			LOG.error("Error while parsing input arguments: {}, {}", e.getClass(), e.getMessage());
			System.exit(-1);
		}

		// initialize the context
			JavaSparkContext ctx = App.initializeSparkContext(props);

		
			if (path != null)
				props.addOrUpdateProperty("dataPath", path);
			else
			{
				LOG.error("Error while moving data into the snapshot directory");
				System.exit(0);
			}
			
			App a = new App();
			
			Metrics metrics = a.new Metrics(props);

			Context ctxTimer = metrics.timer.time();
			JobsStarter starter = new JobsStarter(ctx, props);
			starter.startJobs();
			ctxTimer.stop();
			metrics.reporter.report();
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
//		cfg.setMaster("local");
//		cfg.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		cfg.set("spark.kyro.registrator", "com.vimond.StorageArchitecture.Utils.ClassRegistrator");
//		cfg.set("spark.scheduler.allocation.file", "/var/files/batch/poolScheduler.xml");

		// // ES settings
		cfg.set("es.index.auto.create", "true");
		cfg.set("es.nodes", (String) props.getOrDefault("es.nodes", "localhost"));
		cfg.set("es.input.json", "true");
		for (Tuple2<String, String> prop : cfg.getAll())
			LOG.info(prop._1() + " = " + prop._2());

		return new JavaSparkContext(cfg);

	}

	public void initializeMetrics(AppProperties props)
	{

	}

	private class Metrics
	{
		public transient Timer timer;
		public transient CsvReporter reporter;

		public Metrics(AppProperties props)
		{
			String metricsPath = props.getProperty("metrics.path", "/var/log/hadoop-2.7.0");

			MetricRegistry registry = new MetricRegistry();

			this.timer = registry.timer(new DateTime(DateTimeZone.forID("Europe/Oslo")).toString() + "executionTime");

			reporter = CsvReporter.forRegistry(registry).convertDurationsTo(TimeUnit.MILLISECONDS).convertRatesTo(TimeUnit.SECONDS).build(new File(metricsPath));
		}
	}
}
