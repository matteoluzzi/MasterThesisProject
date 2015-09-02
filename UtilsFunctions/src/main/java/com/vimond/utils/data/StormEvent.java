package com.vimond.utils.data;

import java.io.Serializable;
import java.util.Map;

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
		@SuppressWarnings("unchecked")
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
		@SuppressWarnings("unchecked")
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if (asset_map != null)
			return (String) asset_map.get("appName");
		else
			return null;
	}
	
	@JsonProperty("data.assetId")
	public Integer getAssetId()
	{
		@SuppressWarnings("unchecked")
		Map<String, Object> progress_map = (Map<String, Object>) this.getGenericValues().get("progress");
		if (progress_map != null)
			return (Integer) progress_map.get("assetId");
		else
			return null;
	}
	
	@JsonProperty("data.assetName")
	public String getAssetName()
	{
		@SuppressWarnings("unchecked")
		Map<String, Object> progress_map = (Map<String, Object>) this.getGenericValues().get("progress");
		if (progress_map != null)
			return (String) progress_map.get("title");
		else
			return null;
	}
	
	@JsonProperty("data.userAgent")
	public String getUserAgent()
	{
		@SuppressWarnings("unchecked")
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("client");
		if (asset_map != null)
			return (String) asset_map.get("userAgent");
		else
			return null;
	}
	
	@JsonProperty("data.videoFormat")
	public String getVideoFormat()
	{
		@SuppressWarnings("unchecked")
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("client");
		if (asset_map != null)
			return (String) asset_map.get("video.format");
		else
			return null;
	}

	@JsonProperty("data.playerEvent")
	public String getPlayerEvent()
	{
		@SuppressWarnings("unchecked")
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("client");
		if (asset_map != null)
			return (String) asset_map.get("playerEvent");
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
	
	public void setBrowser(String browser)
	{
		setGenericValue(Constants.BROWSER, browser);
	}
	
	public void setOS(String os)
	{
		setGenericValue(Constants.OS, os);
	}

	public void setCounter()
	{
		setGenericValue(Constants.COUNTER, 1);
	}
}
