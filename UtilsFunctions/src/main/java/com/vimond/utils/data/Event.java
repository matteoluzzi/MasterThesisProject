package com.vimond.utils.data;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.vimond.common.events.data.VimondEventAny;

/**
 * Abstract class representing an input event
 * @author matteoremoluzzi
 *
 */
public abstract class Event extends VimondEventAny
{

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
	
	//Value can be numeric due to a bug in input data
	@SuppressWarnings("unchecked")
	public String getPlayerEvent()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("client");
		if (asset_map != null)
			return String.valueOf(asset_map.get("playerEvent"));
		else
			return null;
	}
	
	public abstract void setCountryName(String countryName);
	
	public abstract void setLatitude(Double latitude);
	
	public abstract void setLongitude(Double longitude);
	
	public abstract void setCity(String city);
	
	public abstract void setBrowser(String browser);
	
	public abstract void setOs(String os);
	
	
}
