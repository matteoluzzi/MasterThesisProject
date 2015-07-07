package no.vimond.RealTimeArchitecture.Utils;

import java.io.IOException;
import java.util.Properties;
import java.util.Map.Entry;

public class KafkaProperties extends Properties {

	private static final long serialVersionUID = 1L;
	
	
	public KafkaProperties()
	{
		this.defaults = new Properties();
		try 
		{
			this.defaults.load(Utility.loadPropertiesFileFromClassPath(Constants.PROPERTIES_FILE));
			for(Entry<Object, Object> entry : defaults.entrySet())
				this.addOrUpdateProperty((String) entry.getKey(), (String) entry.getValue());
			
		}catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void addOrUpdateProperty(String key, String value)
	{
		this.setProperty(key, value);
	}
}
