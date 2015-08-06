package no.vimond.RealTimeArchitecture.Utils;

import java.io.InputStream;

public class Utility
{
	public static InputStream loadPropertiesFileFromClassPath(String filename)
	{
		Thread currentThread = Thread.currentThread();
		ClassLoader contextClassLoader = currentThread.getContextClassLoader();
		InputStream propertiesStream = contextClassLoader.getResourceAsStream(filename);
		return propertiesStream;
	}
}
