package no.vimond.StorageArchitecture;

import java.io.IOException;
import java.util.TimerTask;

import com.backtype.hadoop.pail.Pail;

public class DataPoller extends TimerTask
{
	//the folder where it should read data, if available
	private String folderPath;
	
	public DataPoller(String ...path )
	{
		this.folderPath = String.join("/", path);
	}

	@Override
	public void run()
	{
		try
		{
			Pail<String> pail = Pail.create(this.folderPath, false);
			if(pail.exists("SUCCESS"))
			{
				
			}
			
			
			
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
