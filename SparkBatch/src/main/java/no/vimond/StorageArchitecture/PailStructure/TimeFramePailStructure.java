package no.vimond.StorageArchitecture.PailStructure;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.backtype.hadoop.pail.PailStructure;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class TimeFramePailStructure implements PailStructure<String>
{
	private static final long serialVersionUID = 9195695651901130252L;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private static ObjectMapper mapper = ObjectMapperConfiguration.configure();


	public boolean isValidTarget(String... dirs)
	{
//		if(dirs.length != 4)
//			return false;
//		try
//		{
//			int hour = Integer.parseInt(dirs[1]);
//			int quarter = Integer.parseInt(dirs[2]);
//			return (formatter.parse(dirs[0]) != null && hour >= 0 && hour <= 24 && quarter >= 0 && quarter <= 4);
//		} catch (ParseException e)
//		{
//			return false;
//		}
		return true;
	}

	public String deserialize(byte[] serialized)
	{
		String ret = new String(serialized);
		return ret;
	}

	public byte[] serialize(String object)
	{
		return object.getBytes();
	}

	public List<String> getTarget(String object)
	{
	/*	VimondEventAny event;
		try
		{
			event = mapper.readValue(object, VimondEventAny.class);
			List<String> path = new ArrayList<String>();
			DateTime date = event.getTimestamp();
			path.add(formatter.format(date.toDate()));
			path.add(String.valueOf(date.getHourOfDay()));
			path.add(String.valueOf(date.getMinuteOfHour() / 15));
			return path;
		} catch (JsonParseException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		*/
		DateTime date = new DateTime();
		List<String> path = new ArrayList<String>();
		path.add(formatter.format(date.toDate()));
		path.add(String.valueOf(date.getHourOfDay()));
		path.add(String.valueOf(date.getMinuteOfHour() / 5));
		return path;
		
	}

	public Class getType()
	{
		return String.class;
	}


}
