package no.vimond.StorageArchitecture.HDFS;

import java.io.IOException;

import no.vimond.StorageArchitecture.PailStructure.TimeFramePailStructure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.backtype.hadoop.pail.Pail;

public class DataPoller
{

	private Pail<String> masterDataPail;
	private FileSystem fs;

	@SuppressWarnings("unchecked")
	public DataPoller(String path)
	{
		try
		{
			String hadoop_home = "/Users/matteoremoluzzi/binaries/hadoop-2.7.0";
			Configuration cfg = new Configuration();
			Path sitecorexml = new Path(hadoop_home + "/etc/hadoop/core-site.xml");
			cfg.addResource(sitecorexml);
			fs = FileSystem.get(cfg);
			masterDataPail = new Pail<String>(path);

		} catch (IllegalArgumentException e)
		{
		} catch (IOException e)
		{
		}
	}

	public String ingestNewData()
	{
		// set up a temporary folder for moving the snapshot
		Path tmpPath = new Path("hdfs://localhost:9000/user/matteoremoluzzi/dataset/tmp");
		try
		{
			this.fs.delete(tmpPath, true);
			this.fs.mkdirs(tmpPath);
			// TODO generate a unique folder for the snapshot, in order to have
			// more jobs running on different data
			masterDataPail.snapshot("hdfs://localhost:9000/user/matteoremoluzzi/dataset/tmp/snapshot");
			return tmpPath.toString() + "/snapshot";
		} catch (IOException e)
		{
			return null;
		}
	}

	public Pail<String> getMasterPail()
	{
		return this.masterDataPail;
	}
}
