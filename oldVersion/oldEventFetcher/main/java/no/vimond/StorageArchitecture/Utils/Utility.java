package no.vimond.StorageArchitecture.Utils;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utility
{
	private static Logger LOG = LoggerFactory.getLogger(Utility.class);

	public static InputStream loadPropertiesFileFromClassPath(String filename)
	{
		Thread currentThread = Thread.currentThread();
		ClassLoader contextClassLoader = currentThread.getContextClassLoader();
		InputStream propertiesStream = contextClassLoader.getResourceAsStream(filename);
		return propertiesStream;
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T getPropertyValue(Properties prop, String key)
	{
		Object value = prop.get(key);
		if(value == null)
		{
			LOG.warn("Could not find property value for the key " + key) ;
			return null;
		}
		else
		{
			return (T) value;
		}
			 
	}
}
