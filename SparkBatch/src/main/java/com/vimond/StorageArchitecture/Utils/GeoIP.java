package com.vimond.StorageArchitecture.Utils;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip2.DatabaseReader;

public class GeoIP
{
	private static Logger LOG = LoggerFactory.getLogger(GeoIP.class);

	private static final String DB_NAME = "GeoLite2-Country.mmdb";
	private static GeoIP instance = new GeoIP(DB_NAME);
	private DatabaseReader dbReader;
	private InputStream dbFile;

	private GeoIP(String dbName)
	{
		dbFile = Utility.loadPropertiesFileFromClassPath(dbName);
		if (dbFile != null)
		{
			try
			{
				dbReader = new DatabaseReader.Builder(dbFile).build();
			} catch (IOException e)
			{
				LOG.error("Error while creating dbReader, GeoIP functionality is off");
			}
		} else
		{
			LOG.error("Invalid dbPath, GeoIP functionality is off");
		}
	}
	
	public static DatabaseReader getDbReader()
	{
		return instance.dbReader;
	}
}
