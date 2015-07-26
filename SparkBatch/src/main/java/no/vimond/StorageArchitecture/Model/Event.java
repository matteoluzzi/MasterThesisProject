package no.vimond.StorageArchitecture.Model;

import java.io.Serializable;
import java.util.Map;

import no.vimond.StorageArchitecture.Utils.Constants;

import com.vimond.common.events.data.VimondEventAny;

public class Event extends VimondEventAny implements Serializable
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
	public String getCountryName()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if(asset_map != null)
			return (String) asset_map.get("geo.country");
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
	public Integer getAssetId()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if(asset_map != null)
			return (Integer) asset_map.get("assetId");
		else 
			return null;
	}
	
	@SuppressWarnings("unchecked")
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
	
	
}
