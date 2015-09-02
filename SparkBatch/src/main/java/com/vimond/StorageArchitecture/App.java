package com.vimond.StorageArchitecture;

import java.io.IOException;

import com.vimond.StorageArchitecture.HDFS.DataPoller;
import com.vimond.StorageArchitecture.Utils.AppProperties;
import com.vimond.StorageArchitecture.Utils.Utility;
import com.vimond.utils.data.Constants;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App
{
	private static Logger LOG = LoggerFactory.getLogger(App.class);
	
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
		AppProperties props = new AppProperties();

		if(args.length != 2)
		{
			LOG.error("Missing arguments, exiting now!");
			System.exit(-1);
		}
		
		String path = (String) args[0];
		String freq = args[1];
		
		LOG.info("Folder: " + path);
		LOG.info("Frequency: " + freq);

		final String appName = (String) props.get(Constants.APP_NAME_KEY);

		// Spark settings
		SparkConf cfg = new SparkConf();
		cfg.setAppName(appName);
		cfg.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		cfg.set("spark.kyro.registrator", "com.vimond.StorageArchitecture.Utils.ClassRegistrator");
		cfg.set("spark.default.parallelism", "8");
		
		// ES settings
		cfg.set(Constants.ES_INDEX_AUTO_CREATE_KEY, "true");
		cfg.set("es.nodes", "localhost");
		cfg.set("es.port", "9200");
		cfg.set("es.input.json", "true");

		// spark cluster settings

//		cfg.set("spark.executor.memory", "2g");
//		cfg.set("spark.scheduler.mode", "FAIR");
//		cfg.set("spark.executor.instances", "1");
//		cfg.set("spark.executor.cores", "1");

		JavaSparkContext ctx = new JavaSparkContext(cfg);
		
		DataPoller dataInit = new DataPoller(path);

		if (dataInit.getMasterPail() != null)
		{
			DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/HH/mm");
			
			String folder_path = Utility.extractDate(path);
			props.addOrUpdateProperty("timestamp", formatter.parseDateTime(folder_path));
			props.addOrUpdateProperty("timewindow", freq);
			
			String dataPath = dataInit.ingestNewData();
			
//			String dataPath = path;
			
			if(dataPath != null)
				props.addOrUpdateProperty("dataPath", dataPath);
			else
			{
				LOG.error("Error while moving data into the snapshot directory");
				System.exit(0);
			}

			SimpleJobsStarter starter = new SimpleJobsStarter(ctx, props);
			starter.startJobs();
		}

		else
		{
			LOG.error("Data not aligned with batch program");
			System.exit(0);
		}
	}
}
