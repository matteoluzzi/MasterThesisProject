package com.vimond.StorageArchitecture.HDFS;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.backtype.hadoop.pail.Pail;

public class DataPoller
{

	private Pail<String> masterDataPail;
	private FileSystem fs;

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
		Path tmpPath = new Path("hdfs://localhost:9000/copies");
		try
		{
			this.fs.delete(tmpPath, true);
			this.fs.mkdirs(tmpPath);
			
			UUID uuid = UUID.randomUUID();
			masterDataPail.snapshot("hdfs://localhost:9000/copies/copy" + uuid.toString());
			return tmpPath.toString() + "/copy" + uuid.toString();
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
