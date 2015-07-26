package no.vimond.StorageArchitecture.Utils;

import java.io.IOException;
import java.util.Properties;
import java.util.Map.Entry;

public class AppProperties extends Properties
{

	private static final long serialVersionUID = 2610384692739649930L;

	public AppProperties()
	{
		this.defaults = new Properties();
		try
		{
			this.defaults.load(Utility.loadPropertiesFileFromClassPath(Constants.PROPERTIES_FILE));
			for (Entry<Object, Object> entry : defaults.entrySet())
				this.addOrUpdateProperty((String) entry.getKey(), (String) entry.getValue());

		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public void addOrUpdateProperty(String key, Object value)
	{
		this.put(key, value);
	}
}
