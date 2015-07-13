package no.vimond.StorageArchitecture;

import no.vimond.StorageArchitecture.Model.SimpleModel;
import no.vimond.StorageArchitecture.Utils.AppProperties;
import no.vimond.StorageArchitecture.Utils.Constants;
import no.vimond.StorageArchitecture.Utils.Event;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class App
{

	@SuppressWarnings({ "rawtypes" })
	public static void main(String[] args)
	{

		AppProperties props = new AppProperties();

		final String appName = (String) props.get(Constants.APP_NAME_KEY);

		//final String master = "spark://Matteos-MBP.vimond.local:7077";
		final String master = "local";

		// Spark settings

		SparkConf cfg = new SparkConf();
		cfg.setAppName(appName);
		cfg.setMaster(master);
		
		//ES settings
		cfg.set(Constants.ES_INDEX_AUTO_CREATE_KEY, "true");
		cfg.set("es.nodes","localhost");
		cfg.set("es.port", "9200");
		cfg.set("es.input.json", "true");
		
		//spark cluster settings
		cfg.set("spark.executor.memory", "2g");
		cfg.set("spark.scheduler.mode", "FAIR");
	
		// Complex classes must be (de)serialized with Kyro otherwise it won't
		// work
		Class[] serClasses = { Event.class, SimpleModel.class };
		cfg.registerKryoClasses(serClasses);
		
		JavaSparkContext ctx = new JavaSparkContext(cfg);

		
		
		
		SimpleJobsStarter starter = new SimpleJobsStarter(ctx, props);
		
		while(true)
			starter.startJobs();
		
		//ctx.close();

	}
	
}
