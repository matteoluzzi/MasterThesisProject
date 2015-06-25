package no.vimond.RealTimeArchitecture.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.TestCase;

import org.junit.Test;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;

public class GeoIPTest extends TestCase
{
	public GeoIPTest( String testName )
    {
        super( testName );
    }
	
	/**
	 * Creational test and validation of singleton pattern
	 */
	@Test
	public void testCreation()
	{
		DatabaseReader firstInstance = GeoIP.getDbReader();
		DatabaseReader secondInstance = GeoIP.getDbReader();
		
		assertNotNull(firstInstance);
		assertNotNull(secondInstance);
		
		assertEquals(firstInstance, secondInstance);
	}
	
	@Test
	public void testIpLookUpCountry()
	{
		DatabaseReader dbReader = GeoIP.getDbReader();
		
		try
		{
			InetAddress ipAddress = InetAddress.getByName("10.10.9.92");
			CountryResponse response = dbReader.country(ipAddress);
			Country country = response.getCountry();
			
			assertEquals(country.getName(), "Netherlands");
			
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (GeoIp2Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
}
