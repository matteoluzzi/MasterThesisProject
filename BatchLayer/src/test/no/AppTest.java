package no;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.joda.time.DateTime;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

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

	public void testDate()
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		List<String> path = new ArrayList<String>();
		DateTime date = new DateTime();
		path.add(formatter.format(date.toDate()));
		path.add(String.valueOf(date.getHourOfDay()));
		path.add(String.valueOf(date.getMinuteOfHour() / 15));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testHDFS() throws IOException
	{
		String path = "hdfs://localhost:9000/dataset";
		
		Pail pail = Pail.create(path);
		TypedRecordOutputStream os = pail.openWrite();
        os.writeObject(new byte[] {1, 2, 3});
        os.writeObject(new byte[] {1, 2, 3, 4});
        os.writeObject(new byte[] {1, 2, 3, 4, 5});
        os.close();   
		
	}
	
}