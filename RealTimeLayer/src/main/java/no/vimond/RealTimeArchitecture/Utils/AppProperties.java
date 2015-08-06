package no.vimond.RealTimeArchitecture.Utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Map.Entry;

public class AppProperties extends Properties
{

	private static final long serialVersionUID = 2610384692739649930L;

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
}