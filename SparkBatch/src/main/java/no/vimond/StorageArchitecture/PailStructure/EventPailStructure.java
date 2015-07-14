package no.vimond.StorageArchitecture.PailStructure;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.backtype.hadoop.pail.PailStructure;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimond.common.events.data.VimondEventAny;
import com.vimond.common.shared.ObjectMapperConfiguration;

public class EventPailStructure implements PailStructure<VimondEventAny>
{
	private static final long serialVersionUID = 7869846567202993614L;
	private static ObjectMapper mapper = ObjectMapperConfiguration.configure();
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

	public boolean isValidTarget(String... dirs)
	{
		return true;
	}

	public VimondEventAny deserialize(byte[] serialized)
	{
		String json = new String(serialized);
		try
		{
			return mapper.readValue(json, VimondEventAny.class);
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
	}

	public byte[] serialize(VimondEventAny object)
	{
		try
		{
			return mapper.writeValueAsBytes(object);
		} catch (JsonProcessingException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public List<String> getTarget(VimondEventAny object)
	{
		// TODO Auto-generated method stub
		DateTime date = new DateTime();
		List<String> path = new ArrayList<String>();
		path.add(formatter.format(date.toDate()));
		path.add(String.valueOf(date.getHourOfDay()));
		path.add(String.valueOf(date.getMinuteOfHour() / 5));
		return path;
	}

	public Class getType()
	{
		// TODO Auto-generated method stub
		return VimondEventAny.class;
	}
	
	
}