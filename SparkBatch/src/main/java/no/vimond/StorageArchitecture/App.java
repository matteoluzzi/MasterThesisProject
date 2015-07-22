package no.vimond.StorageArchitecture;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import no.vimond.StorageArchitecture.HDFS.DataPoller;
import no.vimond.StorageArchitecture.Model.SimpleModel;
import no.vimond.StorageArchitecture.Utils.AppProperties;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Utils.Event;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import com.esotericsoftware.minlog.Log;

public class App
{

	@SuppressWarnings({ "rawtypes" })
	public static void main(String[] args) throws IOException, InterruptedException
	{
		AppProperties props = new AppProperties();

		String path = (String) args[0];

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

		// Complex classes must be (de)serialized with Kyro otherwise it won't
		// work
		// Class[] serClasses = { Event.class, SimpleModel.class };
		// cfg.registerKryoClasses(serClasses);

		JavaSparkContext ctx = new JavaSparkContext(cfg);
		
//		props.addOrUpdateProperty("dataPath", path);
//
//		SimpleJobsStarter starter = new SimpleJobsStarter(ctx, props);
//		starter.startJobs();

		DataPoller dataInit = new DataPoller(path);

		if (dataInit.getMasterPail() != null)
		{
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
