package com.vimond.utils.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Map.Entry;

import com.vimond.utils.data.Constants;

/**
 * Class for loading application properties from an external property file
 * @author matteoremoluzzi
 *
 */
public class AppProperties extends Properties
{

	private static final long serialVersionUID = 2610384692739649930L;
	
	public AppProperties()
	{
		this.defaults = new Properties();
		try
		{
			this.defaults.load(AppProperties.loadPropertiesFileFromClassPath(Constants.PROPERTIES_FILE));
			for (Entry<Object, Object> entry : defaults.entrySet())
				this.addOrUpdateProperty((String) entry.getKey(), (String) entry.getValue());

		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public AppProperties(String path) throws FileNotFoundException, IOException
	{
		this.defaults = new Properties();
		
			this.defaults.load(new FileInputStream(path));
			for (Entry<Object, Object> entry : defaults.entrySet())
				this.addOrUpdateProperty((String) entry.getKey(),  entry.getValue());

	}

	public void addOrUpdateProperty(String key, Object value)
	{
		this.put(key, value);
	}
	
	public static InputStream loadPropertiesFileFromClassPath(String filename)
	{
		Thread currentThread = Thread.currentThread();
		ClassLoader contextClassLoader = currentThread.getContextClassLoader();
		InputStream propertiesStream = contextClassLoader
				.getResourceAsStream(filename);
		return propertiesStream;
	}
}