package no.vimond.StorageArchitecture;

import java.io.FileNotFoundException;
import java.io.IOException;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import no.vimond.StorageArchitecture.Consumer.KafkaConsumerHandler;
import no.vimond.StorageArchitecture.PailStructure.TimeFramePailStructure;
import no.vimond.StorageArchitecture.Utils.AppProperties;
import no.vimond.StorageArchitecture.Utils.Constants;

import com.backtype.hadoop.pail.Pail;

/**
 * Project resposible for ingesting the data lake located in
 * hdfs://localhost:9000/dataset with events read from kafka WARNING! USE THE
 * SAME CONFIGURATION FILE AS THE SPARK BATCH PROJECT : java -jar
 * thisProject.jar -conf config.properties
 * 
 * @author matteoremoluzzi
 *
 */
public class App
{

	public static void main(String[] args) throws IOException
	{

		ArgumentParser parser = ArgumentParsers.newArgumentParser("BatchLayer");
		parser.addArgument("-conf", "--configuration").help("Configuration time").type(String.class).required(true).dest("cfg");

		AppProperties props = null;

		try
		{

			Namespace namespace = parser.parseArgs(args);
			String cfg_file = namespace.getString("cfg");

			props = new AppProperties(cfg_file);

		} catch (ArgumentParserException e1)
		{
			parser.printHelp();
		} catch (FileNotFoundException e)
		{
			System.err.println("Configuration file not found");
		}

		if (props != null)
		{
			int batchTimeFrame = (props.get(Constants.TIME_FRAME_KEY) != null) ? Integer.parseInt((String) props.get(Constants.TIME_FRAME_KEY)) : Constants.DEFAULT_TIME_FRAME;;
			try
			{
				Pail.create(Constants.NEW_DATA_PATH, new TimeFramePailStructure(batchTimeFrame));
			} catch (IllegalArgumentException e) // Exception is caught due to
													// an existing pail in that
													// location, just ignore it
			{
			}

			KafkaConsumerHandler handler = new KafkaConsumerHandler(props);

			handler.registerConsumerGroup();
			handler.startListening();
		}
	}
}
