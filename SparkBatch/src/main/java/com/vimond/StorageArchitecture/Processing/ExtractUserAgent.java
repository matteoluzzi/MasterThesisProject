package com.vimond.StorageArchitecture.Processing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import net.sf.uadetector.OperatingSystem;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vimond.StorageArchitecture.Utils.EventInfo;
import com.vimond.utils.data.SparkEvent;
import com.vimond.utils.functions.UserAgentParser;

/**
 * Initialize a userAgent parser is expensive, the best approach is to do it in a mapParition function. 
 * @author matteoremoluzzi
 *
 */
public class ExtractUserAgent implements FlatMapFunction<Iterator<SparkEvent>, EventInfo>
{
	private static UserAgentStringParser uaParser = UADetectorServiceFactory.getResourceModuleParser();
	
	private static final long serialVersionUID = 8557410115713662281L;

	@Override
	public Iterable<EventInfo> call(Iterator<SparkEvent> events) throws Exception
	{
		Collection<EventInfo> result = new ArrayList<EventInfo>();
		
		while(events.hasNext())
		{
			SparkEvent event = events.next();
			String ua = event.getUserAgent();
			ReadableUserAgent userAgent = uaParser.parse(ua);

			OperatingSystem os = userAgent.getOperatingSystem();
			String os_str = os.getName() + " " + os.getVersionNumber().getMajor();

			// TODO handle new user agent from internet explorer 11

			String browser_str = userAgent.getName() + "/" + userAgent.getVersionNumber().getMajor();
			
			EventInfo info = new EventInfo(event.getAssetId(), event.getAssetName(), browser_str, os_str, event.getVideoFormat());
			
			result.add(info);
		}
		
		return result;
		
	}
}
