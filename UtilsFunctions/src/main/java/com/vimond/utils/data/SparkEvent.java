package com.vimond.utils.data;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.vimond.common.events.data.VimondEventAny;

public class SparkEvent extends VimondEventAny implements Serializable
{
	private static final long serialVersionUID = 1733723878642302297L;
	
	
	@SuppressWarnings("unchecked")
	public String getIpAddress()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if(asset_map != null)
			return (String) asset_map.get("ip");
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
	public String getUserAgent()
	{
		Map<String, Object> client_map = (Map<String, Object>) this.getGenericValues().get("client");
		if(client_map != null)
			return (String) client_map.get("userAgent");
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
	public String getPlayerEventType()
	{
		try
		{
		Map<String, Object> client_map = (Map<String, Object>) this.getGenericValues().get("client");
		if(client_map != null)
			return (String) client_map.get("playerEvent");
		else 
			return null;
		}
		catch(Exception e)
		{
			System.out.println("playerEvent : " + ((Map<String, Object>) this.getGenericValues().get("client")).get("playerEvent"));
			e.printStackTrace();
			return null;
		}
	}
	
	@SuppressWarnings("unchecked")
	public String getVideoFormat()
	{
		Map<String, Object> client_map = (Map<String, Object>) this.getGenericValues().get("client");
		if(client_map != null)
			return (String) client_map.get("video.format");
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
	public String getOs()
	{
		Map<String, Object> client_map = (Map<String, Object>) this.getGenericValues().get("client");
		if(client_map != null)
			return (String) client_map.get(Constants.OS);
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
	public String getBrowser()
	{
		Map<String, Object> client_map = (Map<String, Object>) this.getGenericValues().get("client");
		if(client_map != null)
			return (String) client_map.get(Constants.BROWSER);
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
	public Integer getAssetId()
	{
		Map<String, Object> progressMap = (Map<String, Object>) this.getGenericValues().get("progress");
		if(progressMap != null)
			return (Integer) progressMap.get("assetId");
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
	@JsonIgnore
	public String getAppName()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if(asset_map != null)
			return  (String) asset_map.get("appName");
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
	public void setCountryName(String countryName)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		asset_map.put(Constants.COUNTRY_NAME, countryName);
	}
	
	@SuppressWarnings("unchecked")
	public void setLatitude(Double latitude)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		asset_map.put(Constants.LATITUDE, latitude);
	}
	
	@SuppressWarnings("unchecked")
	public void setLongitude(Double longitude)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		asset_map.put(Constants.LONGITUDE, longitude);
	}
	
	@SuppressWarnings("unchecked")
	public void setCity(String city)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		asset_map.put(Constants.LONGITUDE, city);
	}
	
	@SuppressWarnings("unchecked")
	public void setBrowser(String browser)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("client");
		asset_map.put(Constants.BROWSER, browser);
	}
	
	@SuppressWarnings("unchecked")
	public void setOs(String os)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("client");
		asset_map.put(Constants.OS, os);
	}
	
	@SuppressWarnings("unchecked")
	public String getAssetName()
	{
		Map<String, Object> progressMap = (Map<String, Object>) this.getGenericValues().get("progress");
		if(progressMap != null)
			return (String) progressMap.get("title");
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
	public String getCountryName()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if(asset_map != null)
			return (String) asset_map.get("geo.country");
		else 
			return null;
	}
	
}
