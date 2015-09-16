package com.vimond.pailStructure;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import com.backtype.hadoop.pail.PailStructure;

/**
 * PailStructure for storing events in a folder according to the current time. Needed for executing the tests on a simulated stream of data
 * @author matteoremoluzzi
 *
 */
public class TimeFrameCurrentTimePailStructure implements PailStructure<String>
{

	private static final long serialVersionUID = -6254445708519312746L;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private static int timeFrame;

	@Override
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

	@Override
	public String deserialize(byte[] serialized)
	{
		return new String(serialized);
	}

	@Override
	public byte[] serialize(String object)
	{
		return object.getBytes();
	}

	@Override
	public List<String> getTarget(String object)
	{
		List<String> path = new ArrayList<String>();
		DateTime now = new DateTime();
		path.add(formatter.format(now.toDate()));
		path.addAll(getCorrectFolder(now.getHourOfDay(), now.getMinuteOfHour()));
		return path;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getType()
	{
		return String.class;
	}

	public List<String> getCorrectFolder(int hours, int minutes)
	{
		//division of the current hour
		if(TimeFrameCurrentTimePailStructure.timeFrame <= 60)
		{
			int timeFrame = minutes / TimeFrameCurrentTimePailStructure.timeFrame;
			return Arrays.asList(String.format("%02d", hours), String.format("%02d", timeFrame * TimeFrameCurrentTimePailStructure.timeFrame));
		}
		//division of current day
		else
		{
			int current_minutes = hours * 60 + minutes;
			int timeFrame = current_minutes / TimeFrameCurrentTimePailStructure.timeFrame;
			return Arrays.asList(String.format("%02d", timeFrame), "00");
		}
	}
	
	public static void initialize(int timeframe) {
		if(60 % timeframe == 0 || 60 * 24 % timeframe == 0)
			TimeFrameCurrentTimePailStructure.timeFrame = timeframe;
		else
			throw new IllegalArgumentException("Timeframe must be a divisor of the hour or of the day");
	}
	
}
