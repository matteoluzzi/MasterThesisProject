package no.vimond.StorageArchitecture.PailStructure;

import java.util.Collections;
import java.util.List;


import com.backtype.hadoop.pail.PailStructure;

public class NewDataPailStructure implements PailStructure<String>
{
	private static final long serialVersionUID = 7869846567202993614L;
	
	public String deserialize(byte[] arg0)
	{
		return new String(arg0);
	}

	public List<String> getTarget(String arg0)
	{
		return Collections.emptyList();
	}

	public Class getType()
	{
		return String.class;
	}

	public boolean isValidTarget(String... arg0)
	{
		return true;
	}

	public byte[] serialize(String arg0)
	{
		return arg0.getBytes();
	}
}
