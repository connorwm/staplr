package net.staplr.common;

public class Credentials
{
	public enum Properties
	{
		location,
		port,
		key
	}
	
	private Object[] properties;
	
	public Credentials()
	{
		properties = new Object[Properties.values().length];
	}
	
	/**Gets a property
	 * @param property
	 * @return Object
	 * @author murphyc1
	 */
	public synchronized Object get(Properties property)
	{
		return properties[property.ordinal()];
	}
	
	/**Sets a property
	 * @param property
	 * @param value
	 * @author murphyc1
	 */
	public synchronized void set(Properties property, Object value)
	{
		properties[property.ordinal()] = value;
	}
	
	/**Multiline string representation for each property
	 * @author murphyc1
	 */
	public String toString()
	{
		String str_asString = new String();
		
		for(int i_propertyIndex = 0; i_propertyIndex < properties.length; i_propertyIndex++)
		{
			str_asString += Properties.values()[i_propertyIndex]+" = '"+properties[i_propertyIndex]+"'\r\n";
		}
		
		return str_asString;
	}
	
}