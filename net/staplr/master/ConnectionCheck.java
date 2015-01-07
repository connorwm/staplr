package net.staplr.master;

import java.util.ArrayList;

public class ConnectionCheck
{
	private String str_address;
	private String[] arr_expectedResponses;
	private ArrayList<ConnectionCheckResponse> arr_responses;
	
	public ConnectionCheck(String str_address, String[] arr_expectedResponses)
	{
		this.str_address = str_address;
		this.arr_expectedResponses = arr_expectedResponses;
		this.arr_responses = new ArrayList<ConnectionCheckResponse>();
	}
	
	public void addResponse(ConnectionCheckResponse ccr_response)
	{
		arr_responses.add(ccr_response);
	}
	
	public boolean allResponsesReceived()
	{
		return (arr_responses.size() >= arr_expectedResponses.length);
	}
	
	public ArrayList<String> getUnrespondedMasterAddresses()
	{
		ArrayList<String> arr_unresponded = new ArrayList<String>();
		
		for(int i_expectedResponseIndex = 0; i_expectedResponseIndex < arr_expectedResponses.length; i_expectedResponseIndex++)
		{
			boolean b_found = false;
			
			for(int i_responseIndex = 0; i_responseIndex < arr_responses.size(); i_responseIndex++)
			{
				if(arr_responses.get(i_responseIndex).getMasterAddress().equals(arr_expectedResponses[i_expectedResponseIndex]))
				{
					b_found = true;
					break;
				}
			}
			
			if(!b_found)
			{
				arr_unresponded.add(arr_expectedResponses[i_expectedResponseIndex]);
			}
		}
		
		return arr_unresponded;
	}
}