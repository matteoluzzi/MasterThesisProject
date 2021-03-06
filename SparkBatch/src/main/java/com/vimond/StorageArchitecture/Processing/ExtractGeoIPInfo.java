package com.vimond.StorageArchitecture.Processing;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.maxmind.geoip2.DatabaseReader;
import com.vimond.StorageArchitecture.Utils.GeoIP;
import com.vimond.StorageArchitecture.Utils.GeoInfo;
import com.vimond.StorageArchitecture.Utils.Utility;
import com.vimond.utils.data.SparkEvent;


public class ExtractGeoIPInfo implements Function<SparkEvent, SparkEvent>
{

	private static final long serialVersionUID = 5768355406775319971L;
	private boolean dbLiteVersion;
	
	public ExtractGeoIPInfo(Broadcast<Boolean> dbLiteVersion)
	{
		this.dbLiteVersion = dbLiteVersion.getValue();
	}
	

	@Override
	public SparkEvent call(SparkEvent event) throws Exception
	{
		DatabaseReader dbReader = GeoIP.getDbReader();

		String ipAddr = event.getIpAddress();
		if (ipAddr != null)
		{
			GeoInfo geoInfo = Utility.getCountryAndCoordinatesFromIp(dbReader, ipAddr, dbLiteVersion);

			if (geoInfo != null)
			{
				if (geoInfo != null)
				{
					// if(geoInfo.getCountry() != null)
					event.setCountryName(geoInfo.getCountry());
					// if(geoInfo.getCity() != null)
					event.setCity(geoInfo.getCity());
					// if(geoInfo.getLatitute() != null)
					event.setLatitude(geoInfo.getLatitute());
					// if(geoInfo.getLongitude() != null)
					event.setLongitude(geoInfo.getLongitude());
				}
			}
		}
		return event;
	}

}
