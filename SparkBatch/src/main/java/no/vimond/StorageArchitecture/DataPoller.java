package no.vimond.StorageArchitecture;

import java.io.IOException;

import no.vimond.StorageArchitecture.PailStructure.NewDataPailStructure;
import no.vimond.StorageArchitecture.PailStructure.TimeFramePailStructure;
import no.vimond.StorageArchitecture.Utils.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;

public class DataPoller
{

	private Pail<String> masterDataPail;
	private FileSystem fs;

	public DataPoller(String path)
	{
		try
		{
			fs = FileSystem.get(new Configuration());
			masterDataPail = Pail.create(path, new TimeFramePailStructure());

		} catch (IllegalArgumentException e)
		{
			try
			{
				masterDataPail = new Pail(path);
			} catch (IOException e1)
			{
			}
		} catch (IOException e)
		{
		}
	}

	public String ingestNewData()
	{
		// set up a temporary folder for moving the snapshot
		Path tmpPath = new Path("/Users/matteoremoluzzi/dataset/tmp");
		try
		{
			this.fs.delete(tmpPath, true);
			this.fs.mkdirs(tmpPath);

			masterDataPail.snapshot("/Users/matteoremoluzzi/dataset/tmp/snapshot");
			return tmpPath.toString();
		} catch (IOException e)
		{
			return null;
		}
	}
}
