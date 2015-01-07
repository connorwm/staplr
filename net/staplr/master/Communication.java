package net.staplr.master;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.staplr.common.Communicator;
import net.staplr.common.Settings;
import net.staplr.common.Credentials.Properties;
import net.staplr.common.Worker;
import net.staplr.common.message.Message;
import net.staplr.common.message.MessageExecutor;
import net.staplr.logging.Log;
import net.staplr.logging.LogHandle;
import net.staplr.common.message.Message.Type;
import net.staplr.common.message.Message.Value;

public class Communication implements Runnable
{
	private Settings s_settings;
	private ExecutorService es_components;
	
	private Communicator c_service;
	private Communicator c_master;
	
	private LogHandle lh_communication;
	
	private ArrayList<ConnectionCheck> arr_connectionChecks;
	
	public Communication(Settings s_settings, int i_servicePort, int i_masterCommunicationPort, Log l_main, Feeds f_feeds)
	{
		this.s_settings = s_settings;
		
		lh_communication = new LogHandle("com", l_main);
		
		c_service = new Communicator(s_settings, i_servicePort, Communicator.Type.Service, l_main, f_feeds);
		c_master = new Communicator(s_settings, i_masterCommunicationPort, Communicator.Type.Master, l_main, f_feeds);
		es_components = Executors.newCachedThreadPool();
		
		arr_connectionChecks = new ArrayList<ConnectionCheck>();
	}
	
	public void run()
	{
		lh_communication.write("Initiated: starting components...");	
		
		es_components.submit(c_service);
		es_components.submit(c_master);
		
		lh_communication.write("Communication initiated");
	}
	
	public void feedSync(Message msg_feedSync)
	{
		lh_communication.write("Initiating FeedSync with other masters");	
		
		for(Worker w_worker : c_master.getWorkers()) 
		{
			w_worker.mx_executor.send(msg_feedSync);
		}
		
		lh_communication.write("Distributed FeedSync message to "+c_master.getConnectionCount());
	}
	
	/**Sends a message to a fellow master to check and 
	 * @param str_address - Address of master to confirm if down
	 */
	public void verifyDownMaster(String str_address)
	{
		Message m_connectionCheck = new Message(Type.Request, Value.ConnectionCheck);
		m_connectionCheck.addItem("address", str_address);
		
		c_master.broadcast(m_connectionCheck);
		
		ConnectionCheck cc_check = new ConnectionCheck(str_address, (String[])c_master.getConnectedMasters().keySet().toArray());
		arr_connectionChecks.add(cc_check);
	}
	
	public void joinMasters()
	{
		MessageExecutor mx_executor = null;
		Message msg_sync = new Message(Message.Type.Request, Message.Value.Sync);
		lh_communication.write("Attempting to join masters...");
		
		for(int i_masterIndex = 0; i_masterIndex < s_settings.c_credentials.size(); i_masterIndex++)
		{
			lh_communication.write("Connecting to "+s_settings.c_credentials.get(i_masterIndex).get(Properties.location));
			mx_executor = c_master.connect(s_settings.c_credentials.get(i_masterIndex));
			
			if(mx_executor != null)
			{
				lh_communication.write("\tConnection established; sending sync request");
				mx_executor.send(msg_sync);
				
				break;
			}
		}
		
		//l_main.write("ERROR: Could not establish connection to other masters; shutting down...");
	}
	
	public Communicator getMasterCommunicator()
	{
		return c_master;
	}
	
	public Communicator getServiceCommunicator()
	{
		return c_service;
	}
}