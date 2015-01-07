package net.staplr.master;

public class ConnectionCheckResponse
{
	String str_masterAddress;
	String str_status;
	
	public ConnectionCheckResponse(String str_masterAddress, String str_status)
	{
		this.str_masterAddress = str_masterAddress;
		this.str_status = str_status;
	}
	
	public String getMasterAddress()
	{
		return str_masterAddress;
	}
	
	public String getConnectionStatus()
	{
		return str_status;
	}
}