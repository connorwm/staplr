package net.staplr.slave;

/**Object to store a feed's information inbetween the Coordinator and Downloader
 * @author murphyc1
 */
public class UpdatableFeed
{
	public enum Properties
	{
		lastSaveDate,
		url
	}
	
	private Object[] properties;
	
	public UpdatableFeed()
	{
		properties = new Object[Properties.values().length];
	}
	
	public UpdatableFeed(Object[] properties)
	{
		this.properties = properties;
	}
	
	/**Set a property of the feed
	 * @author murphyc1
	 * @param property
	 * @param value
	 */
	public void set(Properties property, Object value)
	{
		properties[property.ordinal()] = value;
	}
	
	/**Get a property of a feed
	 * @author murphyc1
	 * @param property
	 * @return
	 */
	public Object get(Properties property)
	{
		return properties[property.ordinal()];
	}
}