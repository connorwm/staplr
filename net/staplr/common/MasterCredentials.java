package net.staplr.common;

/**Holds credentials for Masters - both service and master-to-master connection details
 */
public class MasterCredentials
{
	public enum Properties
	{
		location,
		servicePort,
		masterPort,
		key
	}
	
	private Object[] properties;
	
	public MasterCredentials()
	{
		properties = new Object[Properties.values().length];
	}
	
	/**Gets a property
	 * @param property location, servicePort, masterPort, key
	 * @return value (type varies depending on property)
	 */
	public Object get(Properties property)
	{
		synchronized (properties)
		{
			return properties[property.ordinal()];
		}
	}

	/**Sets a property
	 * @param property location, servicePort, masterPort, key
	 * @param value (type varies depending on property)
	 */
	public void set(Properties property, Object value)
	{
		synchronized (properties)
		{
			properties[property.ordinal()] = value;
		}
	}
	
	/**Multiline string representation for each property
	 * @return String
	 */
	public String toString()
	{
		String str_asString = new String();
		
		synchronized (properties)
		{
			for(int i_propertyIndex = 0; i_propertyIndex < properties.length; i_propertyIndex++)
			{
				str_asString += Properties.values()[i_propertyIndex]+" = '"+properties[i_propertyIndex]+"'\r\n";
			}
		}
		
		return str_asString;
	}
}