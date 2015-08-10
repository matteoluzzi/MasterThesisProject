package com.vimond.StorageArchitecture;

import java.io.IOException;

import com.vimond.StorageArchitecture.HDFS.DataPoller;
import com.vimond.StorageArchitecture.Utils.AppProperties;
import com.vimond.StorageArchitecture.Utils.Constants;
import com.vimond.StorageArchitecture.Utils.Utility;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.esotericsoftware.minlog.Log;

public class App
{

	public static void main(String[] args) throws IOException, InterruptedException
	{
		AppProperties props = new AppProperties();

		String path = (String) args[0];
		String freq = args[1];

		final String appName = (String) props.get(Constants.APP_NAME_KEY);

		// Spark settings
		SparkConf cfg = new SparkConf();
		cfg.setAppName(appName);
		
		// ES settings
		cfg.set(Constants.ES_INDEX_AUTO_CREATE_KEY, "true");
		cfg.set("es.nodes", "localhost");
		cfg.set("es.port", "9200");
		cfg.set("es.input.json", "true");

		// spark cluster settings

		cfg.set("spark.executor.memory", "2g");
		cfg.set("spark.scheduler.mode", "FAIR");

		JavaSparkContext ctx = new JavaSparkContext(cfg);
		
		DataPoller dataInit = new DataPoller(path);

		if (dataInit.getMasterPail() != null)
		{
			DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/HH/mm");
			
			String folder_path = Utility.extractDate(path);
			props.addOrUpdateProperty("timestamp", formatter.parseDateTime(folder_path));
			props.addOrUpdateProperty("timewindow", freq);
			
			String dataPath = dataInit.ingestNewData();

			props.addOrUpdateProperty("dataPath", dataPath);

			SimpleJobsStarter starter = new SimpleJobsStarter(ctx, props);
			starter.startJobs();
		}

		else
		{
			Log.error("Data not aligned with batch program");
			System.exit(0);
		}
	}
}
