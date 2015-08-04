package no.vimond.RealTimeArchitecture.Utils;

import java.io.Serializable;
import java.util.Map;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vimond.common.events.data.VimondEventAny;

public class StormEvent extends VimondEventAny implements Serializable
{
	private static final long serialVersionUID = 1733723878642302297L;
	
	@JsonProperty
	private long initTime;
	
	@JsonProperty("data.ip")
	public String getIpAddress()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if (asset_map != null)
			return (String) asset_map.get("ip");
		else
			return null;
	}

	@JsonIgnore
	public String getCountryName()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues();
		if (asset_map != null)
			return (String) asset_map.get("geo.country");
		else
			return null;
	}
	
	@JsonProperty("data.appName")
	public String getAppName()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if (asset_map != null)
			return (String) asset_map.get("appName");
		else
			return null;
	}
	
	@JsonProperty("data.assetId")
	public Integer getAssetId()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if (asset_map != null)
			return (Integer) asset_map.get("assetId");
		else
			return null;
	}

	public void setCountryName(String countryName)
	{
		setGenericValue(Constants.COUNTRY_NAME, countryName);
	}

	public void setLatitude(Double latitude)
	{
		setGenericValue(Constants.LATITUDE, latitude);
	}

	public void setLongitude(Double longitude)
	{
		setGenericValue(Constants.LONGITUDE, longitude);
	}

	public void setCity(String city)
	{
		setGenericValue(Constants.CITY, city);
	}

	public void setCounter()
	{
		setGenericValue("counter", 1);
	}
	
	public void setInitTime(long i)
	{
		this.initTime = i;
	}
	
	public long getInitTime()
	{
		return this.initTime;
	}
}
