package com.vimond.pailStructure;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;

import com.backtype.hadoop.pail.PailStructure;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.shared.ObjectMapperConfiguration;

/**
 * Structure for writing an event into the HDFS.<b>
 * The folder is chosen according to the timestamp of the event
 * @author matteoremoluzzi
 *
 */
public class TimeFramePailStructure implements PailStructure<String>
{
	private static final long serialVersionUID = 9195695651901130252L;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	@SuppressWarnings("unused")
	private static ObjectMapper mapper = ObjectMapperConfiguration.configure();
	//TODO export the value in the configuration file
	private static int timeFrame;
	
	
	public TimeFramePailStructure()
	{
		
	}
	
	public TimeFramePailStructure(int timeframe)
	{
		TimeFramePailStructure.timeFrame = timeframe;
	}
	
	public boolean isValidTarget(String... dirs)
	{
		if(dirs.length != 4)
			return false;
		try
		{
			int hour = Integer.parseInt(dirs[1]);
			int quarter = Integer.parseInt(dirs[2]);
			return (formatter.parse(dirs[0]) != null && hour >= 0 && hour <= 24 && quarter >= 0 && quarter < 60);
		} catch (ParseException e)
		{
			return false;
		}
	}

	public String deserialize(byte[] serialized)
	{
		return new String(serialized);
	}

	public byte[] serialize(String object)
	{
		return object.getBytes();
	}

	/**
	 * TODO decomment the code later
	 */
	public List<String> getTarget(String object)
	{
//		VimondEventAny event;
//		try
//		{
//			event = mapper.readValue(object, VimondEventAny.class);
//			List<String> path = new ArrayList<String>();
//			DateTime date = event.getTimestamp();
//			path.add(formatter.format(date.toDate()));
//			path.addAll(getCorrectFolder(date.getHourOfDay(), date.getMinuteOfHour()));
//			return path;
//		} catch (JsonParseException e)
//		{
//		} catch (JsonMappingException e)
//		{
//		} catch (IOException e)
//		{
//		}
//		return null;
		
		DateTime date = new DateTime();
		List<String> path = new ArrayList<String>();
		path.add(formatter.format(date.toDate()));
		path.addAll(getCorrectFolder(date.getHourOfDay(), date.getMinuteOfHour()));
		return path;
		
	}

	@SuppressWarnings("rawtypes")
	public Class getType()
	{
		return String.class;
	}

	
	public List<String> getCorrectFolder(int hours, int minutes)
	{
		//division of the current hour
		if(TimeFramePailStructure.timeFrame <= 60)
		{
			int timeFrame = minutes / TimeFramePailStructure.timeFrame;
			return Arrays.asList(String.valueOf(hours), String.format("%02d", timeFrame * TimeFramePailStructure.timeFrame));
		}
		//division of current day
		else
		{
			int current_minutes = hours * 60 + minutes;
			int timeFrame = current_minutes / TimeFramePailStructure.timeFrame;
			return Arrays.asList(String.format("%02d", timeFrame * TimeFramePailStructure.timeFrame), "00");
		}
	}

}
