package com.vimond.utils.functions;

import java.util.concurrent.TimeUnit;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class UserAgentParser implements UserAgentStringParser
{
	
	private final UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
	private final Cache<String, ReadableUserAgent> cache = CacheBuilder.newBuilder().maximumSize(200).expireAfterAccess(2, TimeUnit.HOURS).build();
	

	public String getDataVersion()
	{
		return parser.getDataVersion();
	}

	public ReadableUserAgent parse(String arg0)
	{
		ReadableUserAgent agent = this.cache.getIfPresent(arg0);
		if(agent == null)
		{
			agent = parser.parse(arg0);
			cache.put(arg0, agent);
		}
		return agent;
	}

	public void shutdown()
	{
		parser.shutdown();
	}

}
