package com.vimond.StorageArchitecture.Utils;

import java.io.Serializable;

public class EventInfo implements Serializable
{

	private static final long serialVersionUID = -4114983711536515775L;
	
	private int assetId;
	private String assetName;
	private String browser;
	private String os;
	private String videoFormat;
	
	public EventInfo(int assetId, String assetName, String browser, String os, String videoFormat)
	{
		this.assetId = assetId;
		this.assetName = assetName;
		this.browser = browser;
		this.os = os;
		this.videoFormat = videoFormat;
	}

	public int getAssetId()
	{
		return assetId;
	}

	public String getAssetName()
	{
		return assetName;
	}

	public String getBrowser()
	{
		return browser;
	}

	public String getOs()
	{
		return os;
	}

	public String getVideoFormat()
	{
		return videoFormat;
	}

	public void setAssetId(int assetId)
	{
		this.assetId = assetId;
	}

	public void setAssetName(String assetName)
	{
		this.assetName = assetName;
	}

	public void setBrowser(String browser)
	{
		this.browser = browser;
	}

	public void setOs(String os)
	{
		this.os = os;
	}

	public void setVideoFormat(String videoFormat)
	{
		this.videoFormat = videoFormat;
	}
	
	

}
