package no.vimond.StorageArchitecture.Utils;

import java.io.Serializable;
import java.util.Map;

import com.vimond.common.events.data.VimondEventAny;

public class StormEvent extends VimondEventAny implements Serializable
{
	private static final long serialVersionUID = 1733723878642302297L;
	
	
	public String getIpAddress()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if(asset_map != null)
			return (String) asset_map.get("ip");
		else 
			return null;
	}
	
	public String getCountryName()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if(asset_map != null)
			return (String) asset_map.get("geo.country");
		else 
			return null;
	}
	
	public Integer getAssetId()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		if(asset_map != null)
			return (Integer) asset_map.get("assetId");
		else 
			return null;
	}
	
	public void setCountryName(String countryName)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		asset_map.put(Constants.COUNTRY_NAME, countryName);
	}
	
	public void setLatitude(Double latitude)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		asset_map.put(Constants.LATITUDE, latitude);
	}
	
	public void setLongitude(Double longitude)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		asset_map.put(Constants.LONGITUDE, longitude);
	}
	
	public void setCity(String city)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		asset_map.put(Constants.LONGITUDE, city);
	}
	
	
}
