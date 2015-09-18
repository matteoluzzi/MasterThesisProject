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
		case STARTS_COUNT:
			return new PlayerStartEventsCounter();
		case TOP_BROWSER:
			return new TopBrowsers();
		case TOP_OS:
			return new TopOs();
		case TOP_VIDEOFORMAT:
			return new TopVideoFormat();
		case ENDS_COUNT:
			return new PlayerEndEventsCounter();
		case TOP_ASSET_END:
			return new TopPlayerEndEventsPerAsset();
		case MATCH_ALL:
			return new MatchAll();
		default:
		break;
		}
		return null;
	}
}
