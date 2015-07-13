package no.vimond.StorageArchitecture;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.backtype.hadoop.pail.PailStructure;

public class MyPailStructure implements PailStructure<String>
{
	private static final long serialVersionUID = 7869846567202993614L;
	
	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

	public String deserialize(byte[] arg0)
	{
		return new String(arg0);
	}

	public List<String> getTarget(String arg0)
	{
		List<String> directoryPath = new ArrayList<String>();
		DateTime date = new DateTime();
		directoryPath.add(formatter.format(date.toDate()));
		directoryPath.add(String.valueOf(date.getHourOfDay()));
		return directoryPath;

	}

	public Class getType()
	{
		return String.class;
	}

	public boolean isValidTarget(String... arg0)
	{
		if (arg0.length != 3)
			return false;
		try
		{
			int hour = Integer.parseInt(arg0[1]);
			return (formatter.parse(arg0[0]) != null && hour >= 0 && hour <= 24);
		} catch (ParseException e)
		{
			return false;
		}
	}

	public byte[] serialize(String arg0)
	{
		return arg0.getBytes();
	}
}
