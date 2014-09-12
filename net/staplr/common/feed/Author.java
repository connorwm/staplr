package net.staplr.common.feed;

public class Author
{
	public static enum Properties
	{
		name,
		email
	}
	
	private String[] properties;
	
	public Author()
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
}