package net.staplr.common.feed;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.simple.JSONObject;

/**Holds a feed's data from the statistics document
 * @author murphyc1
 *
 */
public class Feed
{
	public enum Properties
	{
		name,
		url,
		dateFormat,
		collection,
		ttl,
		timestamp
	}
	
	private String[] str_properties;
	
	public Feed()
	{
		str_properties = new String[Properties.values().length];
	}
	
	public void set(Properties p_property, String str_value)
	{
		str_properties[p_property.ordinal()] = str_value;
	}
	
	public String get(Properties p_property)
	{
		return str_properties[p_property.ordinal()]; 
	}
	
	public DateTime getLastDate()
	{
		DateTime dt_lastDate = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
		
		if(str_properties[Properties.timestamp.ordinal()] != null)
		{
			dt_lastDate = dt_lastDate.plusSeconds(Integer.parseInt(String.valueOf(str_properties[Properties.timestamp.ordinal()])));
		} else {
			dt_lastDate = null;
		}
		
		return dt_lastDate;
	}
	
	public JSONObject toJSON()
	{
		JSONObject json_object = new JSONObject();
		
		for(int i_propertyIndex = 0; i_propertyIndex < str_properties.length; i_propertyIndex++)
		{
			json_object.put(Properties.values()[i_propertyIndex], str_properties[i_propertyIndex]);
		}
		
		return json_object;
	}
}