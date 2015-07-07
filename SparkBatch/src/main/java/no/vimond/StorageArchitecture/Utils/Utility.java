package no.vimond.StorageArchitecture.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;

public class Utility
{
	private static Logger LOG = LoggerFactory.getLogger(Utility.class);

	public static InputStream loadPropertiesFileFromClassPath(String filename)
	{
		Thread currentThread = Thread.currentThread();
		ClassLoader contextClassLoader = currentThread.getContextClassLoader();
		InputStream propertiesStream = contextClassLoader
				.getResourceAsStream(filename);
		return propertiesStream;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getPropertyValue(Properties prop, String key)
	{
		Object value = prop.get(key);
		if (value == null)
		{
			LOG.warn("Could not find property value for the key " + key);
			return null;
		} else
		{
			return (T) value;
		}

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
			LOG.error("UnknownHostException while looking up the ip address");
		} catch (IOException e)
		{
			LOG.error("IOException while looking up the ip address");
		} catch (GeoIp2Exception e)
		{
			LOG.error("GeoIp2Exception while looking up the ip address");
		}
		return null;
	}
}
