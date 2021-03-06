package com.vimond.StorageArchitecture.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;

public class Utility
{
	public static InputStream loadPropertiesFileFromClassPath(String filename)
	{
		Thread currentThread = Thread.currentThread();
		ClassLoader contextClassLoader = currentThread.getContextClassLoader();
		InputStream propertiesStream = contextClassLoader
				.getResourceAsStream(filename);
		return propertiesStream;
	}


	public static GeoInfo getCountryAndCoordinatesFromIp(DatabaseReader dbReader, String ipAddress, boolean liteVersion)
	{
		try
		{
			InetAddress ipAddr = InetAddress.getByName(ipAddress);

			if (liteVersion)
			{
				CountryResponse response = dbReader.country(ipAddr);
				
				return new GeoInfo(response.getCountry().getName());
			} else
			{

				CityResponse response = dbReader.city(ipAddr);
				Country country = response.getCountry();
				Location location = response.getLocation();

				return new GeoInfo(country.getName(), response.getCity()
						.getName(), location.getLatitude(),
						location.getLongitude());

			}
		} catch (UnknownHostException e)
		{
	//	LOG.error("UnknownHostException while looking up the ip address");
		} catch (IOException e)
		{
	//		LOG.error("IOException while looking up the ip address");
		} catch (GeoIp2Exception e)
		{
	//		LOG.error("IOException while looking up the ip address");
		}
		
		return null;
	}
	
	public static String extractDate(String input)
	{
		Pattern p = Pattern.compile("\\d{4}-\\d{2}-\\d{2}/\\d{2}/\\d{2}$");
		Matcher m = p.matcher(input);
		
		if(m.find())
			return m.group();
		else return null;
	}
}
