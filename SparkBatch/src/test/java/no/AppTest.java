package no;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.vimond.StorageArchitecture.Jobs.LoadDataJob;
import com.vimond.utils.data.Constants;
import com.vimond.utils.data.SparkEvent;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
	/**
	 * Create the test case
	 *
	 * @param testName
	 *            name of the test case
	 */


	public AppTest(String testName)
	{
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite(AppTest.class);
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp()
	{
		assertTrue(true);
	}

	public void testConversion() throws JsonParseException, JsonMappingException, IOException
	{
		String message = "{\"eventName\":\"user-asset_playback-event\",\"originator\":\"ibc-api\",\"versions\":[\"1.0\"],\"timestamp\":\"2014-09-12T08:42:05.773Z\",\"guid\":\"dcf7760d-4f97-490c-9ea6-a47fe8d8217b\",\"ipAddress\":\"82.134.26.130\",\"asset_playback\":{\"assetId\":542,\"userId\":-1,\"orderId\":null,\"categoryId\":1215,\"categoryPlatformId\":375,\"ip\":\"82.134.26.130\",\"bandwidth\":0,\"geo.organization\":\"Unknown\",\"geo.isp\":\"Unknown\",\"geo.country\":\"Unknown\",\"videoFileId\":2509,\"playType\":\"ONDEMAND\",\"ispName\":null,\"referrer\":null,\"appName\":\"VTV-HTML\"}}";
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new JodaModule());
		SparkEvent event = mapper.readValue(message, SparkEvent.class);
		assertNotNull(event);
	}

	/*
	 * public void testLocation() {
	 * 
	 * DatabaseReader dbreader = GeoIP.getDbReader();
	 * 
	 * InetAddress addr; CityResponse res = null; try { addr =
	 * InetAddress.getByName("82.134.26.130"); res = dbreader.city(addr); }
	 * catch (UnknownHostException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); } catch (IOException e) { // TODO Auto-generated
	 * catch block e.printStackTrace(); } catch (GeoIp2Exception e) { res =
	 * null; }
	 * 
	 * assertNotNull(res);
	 * 
	 * }
	 */


	public void testConversion_()
	{
		String message = "SEQ\"org.apache.hadoop.io.BytesWritable!org.apache.hadoop.io.NullWritable      ÅÆLv««^ði4  tÀ  tÀ  t¼{\"eventName\":\"user-as\"}";
		int index = message.indexOf("{");
		System.out.println(index);
	}

	public List<String> UpdateFolderByTime(List<String> path, int timeFrame)
	{
		if (path.size() != 3)
			System.exit(0);

		DateTime date = new DateTime(path.get(0));
		int hourFrame = Integer.parseInt(path.get(1));
		int minuteFrame = Integer.parseInt(path.get(2));
		minuteFrame += 1;
		// must switch the hour
		if (minuteFrame % ((Integer) 60 / timeFrame) == 0)
		{
			minuteFrame = 0;
			hourFrame += 1;
			if (hourFrame % 24 == 0)// must switch the date
			{
				hourFrame = 0;
				date.plusDays(1);
			}
		}

		path.set(0, new SimpleDateFormat("yyyy-MM-dd").format(date.toDate()));
		path.set(1, String.valueOf(hourFrame));
		path.set(2, String.valueOf(minuteFrame));

		return path;
	}

	public void testUpdateTimeFolder()
	{
		int timeFrame = 5;

		List<String> path = new ArrayList<String>();
		DateTime now = new DateTime();
		path.add(new SimpleDateFormat("yyyy-MM-dd").format(now.toDate()));
		path.add(String.valueOf(now.getHourOfDay()));
		path.add(String.valueOf((int) now.getMinuteOfHour() / timeFrame));

		path = UpdateFolderByTime(path, timeFrame);

		System.out.println(path);

	}

	public void testEventConversion() throws JsonParseException, JsonMappingException, IOException
	{
		String message = "{\"eventName\":\"user-asset_playback-event\",\"originator\":\"ibc-api\",\"versions\":[\"1.0\"],\"timestamp\":\"2014-09-12T08:42:05.773Z\",\"guid\":\"dcf7760d-4f97-490c-9ea6-a47fe8d8217b\",\"ipAddress\":\"82.134.26.130\",\"asset_playback\":{\"assetId\":542,\"userId\":-1,\"orderId\":null,\"categoryId\":1215,\"categoryPlatformId\":375,\"ip\":\"82.134.26.130\",\"bandwidth\":0,\"geo.organization\":\"Unknown\",\"geo.isp\":\"Unknown\",\"geo.country\":\"Unknown\",\"videoFileId\":2509,\"playType\":\"ONDEMAND\",\"ispName\":null,\"referrer\":null,\"appName\":\"VTV-HTML\"}}";

		ObjectMapper m = new ObjectMapper();
		m.registerModule(new JodaModule());
		SparkEvent e = m.readValue(message, SparkEvent.class);
		System.out.println(e);

	}
}
