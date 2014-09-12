package net.staplr.master;

import FFW.Network.DefaultSocketConnection;

public class Connection
{
	private DefaultSocketConnection sc_client;
	private MasterCredentials mc_credentials;
	
	public Connection(MasterCredentials mc_credentials, DefaultSocketConnection sc_client)
	{
		this.mc_credentials = mc_credentials;
		this.sc_client = sc_client;
	}
	
	public DefaultSocketConnection getConnection()
	{
		return sc_client;
	}
	
	public MasterCredentials getCredentials()
	{
		return mc_credentials;
	}
}