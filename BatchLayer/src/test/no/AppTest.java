package no;

import java.io.IOException;
import java.text.SimpleDateFormat;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import no.vimond.StorageArchitecture.MyPailStructure;

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

	public void testDateFolder()
	{
		DateTime date = new DateTime();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		String string_date = formatter.format(date.toDate());
		assertEquals(string_date, "2015-07-12");
	}

	public void testPail() throws IOException
	{
		Pail<String> pail = Pail.create("/Users/matteoremoluzzi/testPailFolder", new MyPailStructure());
		
		

		String records = "record1\nrecord2\nrecord3";

		TypedRecordOutputStream os = pail.openWrite();

		os.writeObject(records);
		os.close();

		Pail<String> newPail = new Pail<String>("/Users/matteoremoluzzi/testPailFolder");

		newPail.consolidate();
		
		String records_ = "record1\nrecord2\nrecord3";

		TypedRecordOutputStream os_ = pail.openWrite();

		os_.writeObject(records_);
		os_.close();
		
		

	}

}