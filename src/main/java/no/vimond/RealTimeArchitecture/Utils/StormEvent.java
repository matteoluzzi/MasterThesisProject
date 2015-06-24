package no.vimond.RealTimeArchitecture.Utils;

import java.io.Serializable;
import java.util.Map;

import com.vimond.common.events.data.VimondEventAny;

public class StormEvent extends VimondEventAny implements Serializable
{
	private static final long serialVersionUID = 1733723878642302297L;
	
	private static final String COUNTRY_NAME = "countryName";

	public String getIpAddress()
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		return (String) asset_map.get("ip");
	}
	
	public void setCountryName(String countryName)
	{
		Map<String, Object> asset_map = (Map<String, Object>) this.getGenericValues().get("asset_playback");
		asset_map.put(COUNTRY_NAME, countryName);
	}
}
