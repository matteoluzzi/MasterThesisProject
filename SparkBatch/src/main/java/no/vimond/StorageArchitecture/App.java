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

public class App
{

	@SuppressWarnings({ "rawtypes" })
	public static void main(String[] args) throws IOException, InterruptedException
	{
		AppProperties props = new AppProperties();
		int minBatch = Integer.parseInt((String) props.get("minBatch"));

		List<String> path = new ArrayList<String>();
		path.add("hdfs://localhost:9000");
		path.add("dataset");
		path.add("master");

		DateTime now = new DateTime();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		//
		path.add(formatter.format(now.toDate()));
		path.add(String.valueOf(now.getHourOfDay() - 2));
		path.add(String.valueOf(2));

		final String appName = (String) props.get(Constants.APP_NAME_KEY);

		final String master = "spark://Matteos-MBP.vimond.local:7077";
		//final String master = "local";

		// Spark settings

		SparkConf cfg = new SparkConf();
		cfg.setAppName(appName);
		cfg.setMaster(master);

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
		Class[] serClasses = { Event.class, SimpleModel.class };
		cfg.registerKryoClasses(serClasses);

		JavaSparkContext ctx = new JavaSparkContext(cfg);

		while(true)
		{
			DataPoller dataInit = new DataPoller(String.join("/", path));

			String dataPath = dataInit.ingestNewData();
			
			props.addOrUpdateProperty("dataPath", dataPath);
			
			SimpleJobsStarter starter = new SimpleJobsStarter(ctx, props);
			starter.startJobs();

			path = updateFolderPath(path, minBatch);
			Thread.sleep(minBatch * 60 * 1000);
		}

	}

	public static List<String> updateFolderPath(List<String> path, int timeFrame)
	{
		if (path.size() != 6)
			System.exit(0);

		DateTime date = new DateTime(path.get(3));
		int hourFrame = Integer.parseInt(path.get(4));
		int minuteFrame = Integer.parseInt(path.get(5));
		minuteFrame += 1;
		// must switch the hour
		if (minuteFrame % ((Integer) 60 / timeFrame) == 0)
		{
			minuteFrame = 0;
			hourFrame += 1;
			if (hourFrame % 24 == 0)// must switch the date
			{
				hourFrame = 0;
				date.plusDays(1);
			}
		}

		path.set(3, new SimpleDateFormat("yyyy-MM-dd").format(date.toDate()));
		path.set(4, String.valueOf(hourFrame));
		path.set(5, String.valueOf(minuteFrame));

		return path;
	}
}
