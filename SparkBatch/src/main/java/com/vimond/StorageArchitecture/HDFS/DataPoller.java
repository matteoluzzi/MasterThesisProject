package com.vimond.StorageArchitecture.HDFS;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.backtype.hadoop.pail.Pail;
import com.vimond.utils.config.AppProperties;
//TODO check for bugs, not in use now
public class DataPoller
{

	private Pail<String> masterDataPail;
	private FileSystem fs;
	private final AppProperties props;

	public DataPoller(AppProperties props)
	{
		this.props = props;
		
		try
		{
			String hadoop_home = (String) this.props.getOrDefault("hadoop_home", "/Users/matteoremoluzzi/binaries/hadoop-2.7.0");
			Configuration cfg = new Configuration();
			Path sitecorexml = new Path(hadoop_home + "/etc/hadoop/core-site.xml");
			cfg.addResource(sitecorexml);
			fs = FileSystem.get(cfg);
			String path = (String) this.props.getOrDefault("path", "hdfs://localhost:9000/user/matteoremoluzzi/master");
			
			masterDataPail = new Pail<String>(path);

		} catch (IllegalArgumentException e)
		{
		} catch (IOException e)
		{
		}
	}

	public String ingestNewData()
	{
		
		String snapShotdir = (String) this.props.getOrDefault("snapshotFolder", "hdfs://localhost:9000/user/matteoremoluzzi/copies");
		
		// set up a temporary folder for moving the snapshot
		Path tmpPath = new Path(snapShotdir);
		try
		{
			this.fs.delete(tmpPath, true);
			this.fs.mkdirs(tmpPath);
			
			UUID uuid = UUID.randomUUID();
			masterDataPail.snapshot(snapShotdir + uuid.toString());
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
