package net.staplr.common.message;

import java.util.ArrayList;

import net.staplr.common.Settings;
import net.staplr.common.Settings.Setting;
import net.staplr.common.feed.Feed;

import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class Message
{
	public enum Type{
		Request,
		Response
	}
	public enum Value{
		Authorization,
		Accepted,
		Rejected,
		Sync,
		FeedDistribute,
		Feeds,
		FeedSync,
		Invalid,
		DeliveryNotice,
		DeliveryNoticeRequest,
		ResponseRequest,
		NeverReceived,
		UpdateSettings,
		Settings,
		Disconnect,
		GarbageCollection,
		Logs,
		Ping,
		Pong,
		RedistributeNumber
	}
	
	private Type t_type;
	private Value v_value;
	private String[] str_feeds;
	private String str_message;
	private JSONObject json_message;
	
	private boolean b_isValid;
	private boolean b_isDelivered;
	private boolean b_receivedResponse;
	private boolean b_requiresResponse;
	private boolean b_respondedTo;
	
	private DateTime dt_timeSent;
	private DateTime dt_timeReceived;
	
	public Message(String str_message)
	{
		dt_timeReceived = DateTime.now();
		dt_timeSent = null;
		
		this.str_message = str_message;
		
		b_isValid = true;
		b_isDelivered = false;
		b_receivedResponse = false;
		b_requiresResponse = false;
		
		// Unescape \" dem thangs
//		while(str_message.contains("\\\""))
//		{
//			str_message = str_message.replace("\\\"", "\"");
//		}
		
		try{
			json_message = (JSONObject)JSONValue.parse(str_message);		
		} catch (Exception e) {
			e.printStackTrace();
			b_isValid = false;
		} finally {
			if(json_message.containsKey("Request"))
			{
				t_type = Type.Request;
				
				String str_value = (String)json_message.get("Request");
				
				// Now find the value
				for(int i_valueIndex = 0; i_valueIndex < Value.values().length; i_valueIndex++)
				{
					//System.out.println("Comparing '"+str_value+"' to '"+Value.values()[i_valueIndex].toString()+"'");
					if(str_value.equals(Value.values()[i_valueIndex].toString()))
					{
						v_value = Value.values()[i_valueIndex];
						break;
					}
				}
			} else if (json_message.containsKey("Response")){
				t_type = Type.Response;
				
				// Now find the value
				for(int i_valueIndex = 0; i_valueIndex < Value.values().length; i_valueIndex++)
				{
					String str_value = (String)json_message.get("Response");
					
					//System.out.println("Comparing '"+str_value+"' to '"+Value.values()[i_valueIndex].toString()+"'");
					if(str_value.equals(Value.values()[i_valueIndex].toString()))
					{
						v_value = Value.values()[i_valueIndex];
						break;
					}
				}
			} else {
				// Unknown message type
				System.out.println("Unknown message type");
				b_isValid = false;
			}
		}
	}
	
	public Message(Type t_type, Value v_value)
	{
		dt_timeReceived = null;
		dt_timeSent = null;
		
		json_message = new JSONObject();
		
		this.t_type = t_type;
		this.v_value = v_value;
		
		b_isValid = true;
		b_isDelivered = false;
		b_receivedResponse = false;
		
		switch (t_type)
		{
		case Request:
			b_requiresResponse = true;
			break;
			
		case Response:
			b_requiresResponse = false;
			break;
		};
		
		json_message.put(t_type.toString(), v_value.toString());
		str_message = json_message.toString();
	}
	
	public void addItem(String str_name, String str_value)
	{
		json_message.put(str_name, str_value);
		str_message = json_message.toString();
	}
	
	public void addItem(String str_name, JSONObject json_value)
	{
		json_message.put(str_name, json_value);
		str_message = json_message.toString();
	}
	
	public void addItem(String str_name, JSONArray json_value)
	{
		json_message.put(str_name,  json_value);
		str_message = json_message.toString();
	}
	
	public void addFeeds(String str_name, ArrayList<Feed> arr_list)
	{
		JSONArray json_list = new JSONArray();
		
		for(Feed f_feed : arr_list)
		{
			json_list.add(f_feed.toJSON());
		}
		
		json_message.put(str_name, json_list);
		str_message = json_message.toString();
	}
	
	public boolean isValid()
	{
		return b_isValid;
	}
	
	public String toString()
	{
		//System.out.println(json_message);
		return json_message+"\r\n";
	}
	
	public JSONObject toJSON()
	{
		return json_message;
	}
	
	public Type getType()
	{
		return t_type;
	}
	
	public Value getValue()
	{
		return v_value;
	}
	
	public Object get(String str_name)
	{
		Object o_object = json_message.get(str_name);
		
		/*
		// Now figure out if its an array, object, or just string
		if(JSONArray.class.isInstance(o_object))
		{
			System.out.println("Instance of Array");
			JSONArray arr_json = (JSONArray)o_object;
			ArrayList<String> arr_string = new ArrayList<String>();
			
			for(int i_itemIndex = 0; i_itemIndex < arr_json.size(); i_itemIndex++)
			{
				arr_string.add(arr_json.get(i_itemIndex).toString());
			}
			
			return arr_string;
			return arr_json;
		} else if (JSONObject.class.isInstance(o_object)) {
			System.out.println("Instance of Object");
			return o_object;
		} else {
			return o_object.toString();
		}
		*/
		
		return o_object;
	}	
	
	//
	// Set
	// 
	
	public void setDelivered()
	{
		b_isDelivered = true;
	}
	
	public void setReceivedResponse()
	{
		b_receivedResponse = true;
	}
	
	public void setRequiresResponse()
	{
		b_requiresResponse = true;
	}
	
	public void markAsSent()
	{
		dt_timeSent = DateTime.now();
	}
	
	public void setRespondedTo()
	{
		b_respondedTo = true;
	}
	
	//
	// Get
	//
	
	public boolean isDelivered()
	{
		return b_isDelivered;
	}
	
	public boolean hasReceivedResponse()
	{
		return b_receivedResponse;
	}
	
	public boolean requiresResponse()
	{
		return b_requiresResponse;
	}
	
	public boolean respondedTo()
	{
		return b_respondedTo;
	}
	
	public DateTime getTimeSent()
	{
		return dt_timeSent;
	}
	
	public DateTime getTimeReceived()
	{
		return dt_timeReceived;
	}
}