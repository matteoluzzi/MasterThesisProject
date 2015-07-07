package no.vimond.StorageArchitecture.Model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SimpleJobModel implements Serializable
{

	
	private static final long serialVersionUID = 8643638200824734736L;
	
	@JsonProperty
	private String modelName;
	
	@JsonProperty
	private DateTime timestamp;
	
	@JsonIgnore
	private Map<String, Integer> ranking = new LinkedHashMap<String, Integer>();
	
	public String getModelName()
	{
		return modelName;
	}

	public DateTime getTimestamp()
	{
		return timestamp;
	}

	public void setModelName(String modelName)
	{
		this.modelName = modelName;
	}

	public void setTimestamp(DateTime timestamp)
	{
		this.timestamp = timestamp;
	}

	@JsonAnySetter
	public void setRankingValue(String key, Integer value)
	{
		this.ranking.put(key, value);
	}
	
	@JsonAnyGetter
	public Map<String, Integer> getRanking()
	{
		return this.ranking;
	}
	
}
