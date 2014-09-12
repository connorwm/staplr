package net.staplr.common.feed;

public class Link
{
	public static enum Properties
	{
		href,
		type,
		rel
	}
	
	private String[] properties;
	
	public Link()
	{
		properties = new String[Properties.values().length];
	}
	
	public void set(Properties property, String value)
	{
		properties[property.ordinal()] = value;
	}
	
	public String get(Properties property)
	{
		return properties[property.ordinal()];
	}
	
	public String toString()
	{
		String value = "{";
		
		for(int propertyIndex = 0; propertyIndex < properties.length; propertyIndex++)
		{
			value+=properties[propertyIndex];
			if(propertyIndex+1 != properties.length) value += ", "; 
		}
		
		value += "}";
		return value;
	}
}
