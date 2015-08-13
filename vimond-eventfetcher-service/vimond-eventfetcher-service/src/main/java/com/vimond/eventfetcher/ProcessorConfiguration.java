package com.vimond.eventfetcher;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration class for the message processor
 * @author matteoremoluzzi
 *
 */
public class ProcessorConfiguration
{

	@JsonProperty("id")
	private String processorId;
	
	@JsonProperty("config")
	private Map<String, String> config;

	public String getProcessorId()
	{
		return processorId;
	}

	public Map<String, String> getConfig()
	{
		return config;
	}

	public void setProcessorId(String processorId)
	{
		this.processorId = processorId;
	}

	public void setConfig(Map<String, String> config)
	{
		this.config = config;
	}
	
}
