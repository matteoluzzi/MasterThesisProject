package com.vimond.pailStructure;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;

import com.backtype.hadoop.pail.PailStructure;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.common.events.data.VimondEventAny;

public class EventPailStructure implements PailStructure<VimondEventAny>
{
	private static final long serialVersionUID = 7869846567202993614L;
	private static Logger LOG = org.slf4j.LoggerFactory.getLogger(EventPailStructure.class);
	
	private static ObjectMapper mapper;
	static{
		mapper = new ObjectMapper();
		mapper.registerModule(new JodaModule());
	}
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private int timeFrame;
	
	public EventPailStructure()
	{
	}
	
	public EventPailStructure(int timeFrame)
	{
		this.timeFrame = timeFrame;
	}

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
			LOG.error("Error while deserializing the event, going to skip it");
		} catch (JsonMappingException e)
		{
			LOG.error("Error while deserializing the event, going to skip it");
		} catch (IOException e)
		{
			LOG.error("Error while deserializing the event, going to skip it");
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
			LOG.error("Error while serializing the event, going to skip it");
		}
		return null;
	}

	public List<String> getTarget(VimondEventAny object)
	{
		DateTime date = new DateTime();
		List<String> path = new ArrayList<String>();
		path.add(formatter.format(date.toDate()));
		path.add(String.valueOf(date.getHourOfDay()));
		path.add(String.valueOf(date.getMinuteOfHour() / timeFrame));
		return path;
	}

	public Class<VimondEventAny> getType()
	{
		return VimondEventAny.class;
	}
}