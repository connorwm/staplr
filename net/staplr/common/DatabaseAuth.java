package net.staplr.common;

public class DatabaseAuth
{
	public enum Properties
	{
		location,
		port,
		username,
		password,
		database
	}
	
	private Object[] properties;
	
	public DatabaseAuth()
	{
		properties = new Object[Properties.values().length];
	}
	
	public synchronized void set(Properties property, Object value)
	{
		properties[property.ordinal()] = value;
	}
	
	public synchronized Object get(Properties property)
	{
		return properties[property.ordinal()];
	}
	
	public String toString()
	{
		String str_result = "{\r\n";
		
		for(int i_propertyIndex= 0; i_propertyIndex < properties.length; i_propertyIndex++)
		{
			str_result +="\t\""+Properties.values()[i_propertyIndex].toString()+"\":\""+properties[i_propertyIndex]+"\"\r\n";
		}
		
		str_result+="}";
		
		return str_result;
	}
	
	public boolean isComplete()
	{
		boolean b_isComplete = true;
		
		for(int i_propertyIndex = 0; i_propertyIndex < properties.length; i_propertyIndex++)
		{
			if(properties[i_propertyIndex] == null) 
			{
				b_isComplete = false;
				break;
			}
		}
		
		return b_isComplete;
	}
}