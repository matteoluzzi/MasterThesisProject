package com.vimond.utils.functions.query;

public class QueryFactory
{
	public static QueryFactory instance = new QueryFactory();
	
	public QueryFactory()
	{
	}
	
	public static QueryFactory getFactory()
	{
		return instance;
	}
	
	public Query createQuery(QueryType type)
	{
		switch (type)
		{
		case TOP_ASSET_START:
			return new TopPlayerStartEventsPerAsset();
		default:
		break;
		}
		return null;
	}
}
