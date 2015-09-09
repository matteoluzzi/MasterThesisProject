package com.vimond.utils.data;

import java.io.Serializable;
import java.util.Map;

public class SparkEvent extends Event implements Serializable
{
	private static final long serialVersionUID = 1733723878642302297L;
	
	
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
}
