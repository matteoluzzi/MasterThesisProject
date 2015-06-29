package no.vimond.RealTimeArchitecture.Utils;

import java.util.Optional;

import com.maxmind.geoip2.record.City;

public class GeoInfo
{
	private String country;
	private String city;
	private Double latitute;
	private Double longitude;
	
	public GeoInfo(String isoCode, String city, Double latitude,
			Double longitude)
	{
		this.country = isoCode;
		this.city = city;
		this.latitute = latitude;
		this.longitude = longitude;	
	}

	public GeoInfo(String isoCode)
	{
		this.country = isoCode;
		this.city = null;
		this.latitute = null;
		this.longitude = null;
	}

	public String getCountry()
	{
		return country;
	}

	public String getCity()
	{
		return city;
	}

	public Double getLatitute()
	{
		return latitute;
	}

	public Double getLongitude()
	{
		return longitude;
	}
}
