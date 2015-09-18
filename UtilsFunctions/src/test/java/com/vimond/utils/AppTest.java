package com.vimond.utils;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.vimond.utils.functions.ElasticSearchQueryInterface;
import com.vimond.utils.functions.query.Query;
import com.vimond.utils.functions.query.QueryFactory;
import com.vimond.utils.functions.query.QueryType;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{

	private ElasticSearchQueryInterface e;
	
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
    
    public void testMatchAllQuery()
    {
    	e = new ElasticSearchQueryInterface("localhost");
    	
    	Query q = QueryFactory.getFactory().createQuery(QueryType.MATCH_ALL);
    	
    	q.executeMultiple(e.getClient(), "vimond-realtime", 1000, 1, false);
    	
    	long hits = q.getHits();
    	
    	assertEquals(1820710, hits);
    }
}
