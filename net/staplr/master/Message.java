package net.staplr.master;

import java.util.ArrayList;

import net.staplr.common.Settings;
import net.staplr.common.Settings.Setting;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class Message
{
	enum Type{
		Request,
		Response
	}
	enum Value{
		Authorization,
		Accepted,
		Rejected,
		Sync,
		JoinSync,
		SyncOk,
		Hold,
		FeedDistribute,
		Feeds,
		FeedSync,
		FeedsOk,
		Invalid,
		DeliveryNotice,
		DeliveryNoticeRequest,
		ResponseRequest,
		NeverReceived
	}
	
	private Type t_type;
	private Value v_value;
	private String[] str_feeds;
	private String str_message;
	private JSONObject json_message;
	private Settings s_settings;
	
	private boolean b_isValid;
	private boolean b_isDelivered;
	private boolean b_receivedResponse;
	
	public Message(String str_message, Settings s_settings)
	{
		this.str_message = str_message;
		this.s_settings = s_settings;		
		
		b_isValid = true;
		b_isDelivered = false;
		b_receivedResponse = false;
		
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
					System.out.println("Comparing '"+str_value+"' to '"+Value.values()[i_valueIndex].toString()+"'");
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
					
					System.out.println("Comparing '"+str_value+"' to '"+Value.values()[i_valueIndex].toString()+"'");
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
	
	public Message(Type t_type, Value v_value, Settings s_settings)
	{
		this.s_settings = s_settings;
		json_message = new JSONObject();
		
		this.t_type = t_type;
		this.v_value = v_value;
		
		json_message.put(t_type.toString(), v_value.toString());
		str_message = json_message.toString();
	}
	
	public void addItem(String str_name, String str_value)
	{
		json_message.put(str_name, str_value);
		str_message = json_message.toString();
	}
	
	public void addItem(String str_name, JSONObject o_value)
	{
		json_message.put(str_name, o_value);
		str_message = json_message.toString();
	}
	
	public void addList(String str_name, ArrayList arr_list)
	{
		JSONArray json_list = new JSONArray();
		
		json_list.addAll(arr_list);

		json_message.put(str_name, json_list);
		str_message = json_message.toString();
	}
	
	public boolean isValid()
	{
		return b_isValid;
	}
	
	public String toString()
	{
		System.out.println(json_message);
		return json_message+"\r\n";
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
	
	public void setDelivered()
	{
		b_isDelivered = true;
	}
	
	public void setReceivedResponse()
	{
		b_receivedResponse = true;;
	}
	
	public boolean isDelivered()
	{
		return b_isDelivered;
	}
	
	public boolean hasReceivedResponse()
	{
		return b_receivedResponse;
	}
}