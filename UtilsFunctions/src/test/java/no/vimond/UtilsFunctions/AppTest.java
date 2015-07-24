package no.vimond.UtilsFunctions;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    public void testFromFolderToDate()
    {
    	
    	String folder_path = "2015-07-23/12/10";
    	int timeframe = 5;
    	
    	UpdateRecords ur = new UpdateRecords();
    	
    	ur.deleteRecordsFromES(folder_path, timeframe);
    	
    	
    	
 
    }
}
