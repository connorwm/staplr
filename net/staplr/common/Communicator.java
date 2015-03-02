package net.staplr.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.staplr.common.MasterCredentials.Properties;
import net.staplr.common.message.Message;
import net.staplr.common.message.MessageExecutor;
import net.staplr.common.message.Message.Value;
import net.staplr.common.Settings;
import net.staplr.logging.Log;
import net.staplr.logging.Entry;
import net.staplr.logging.LogHandle;
import net.staplr.master.Communication;
import net.staplr.master.Feeds;
import FFW.Network.*;

/**Master object for handling a specific type of master communications: for either masters and service
 * @author murphyc1
 */
public class Communicator implements Runnable
{
	private Communication c_communication;
	private DefaultSocketConnection sc_listener;
	private Settings s_settings;
	private boolean b_run;
	
	private ExecutorService es_workerPool;
	private ArrayList<Worker> arr_worker;
	private ArrayList<DefaultSocketConnection> arr_client;
	
	private LogHandle lh_communication;
	private Log l_main;
	private Type t_type;
	private Feeds f_feeds;
	
	public enum Type
	{
		Service,
		Master
	}
	
	/**Instantiates the Communicator object 
	 * @param s_settings - Handle to the Settings object
	 * @param i_port - Port to listen on for connections
	 * @param t_type - Type of communicator (Service, Master)
	 * @param l_main - Main log object
	 * @param f_feeds - Object of feeds this master is responsible for
	 * @author murphyc1
	 */
	public Communicator(Communication c_communication, Settings s_settings, int i_port, Type t_type, Log l_main, Feeds f_feeds)
	{
		this.c_communication = c_communication;
		this.s_settings = s_settings;
		sc_listener = new DefaultSocketConnection(i_port);
		this.t_type = t_type;		
		lh_communication = new LogHandle("comr", l_main);
		this.l_main = l_main;
		this.f_feeds = f_feeds;
		
		es_workerPool = Executors.newCachedThreadPool();
		b_run = true;
		arr_worker = new ArrayList<Worker>();
		arr_client = new ArrayList<DefaultSocketConnection>();

		// TODO
		if(sc_listener.getLastError() != null){
			lh_communication.write(Entry.Type.Error, sc_listener.getLastError().toString());
		}
	}
	
	/**Main function of Communicator. Contains the loop for waiting and taking in clients.
	 * @author murphyc1
	 */
	public void run()
	{
		if(sc_listener.isBound())
		{
			lh_communication.write("Bound on port "+sc_listener.getPort());
			
			while(b_run)
			{
				addClient(sc_listener.accept(), false);
			}
		} else {
			lh_communication.write(Entry.Type.Error, "Listener not bound for "+t_type.toString() + " on port " + sc_listener.getPort());
		}
	}
	
	/**Stops the Communicator
	 * @author murphyc1
	 */
	public synchronized void stop()
	{
		b_run = false;
		
		// TODO Cleanup time!
	}
	
	/**Adds a client to the master
	 * @param sc_newClient - Socket connection object of the connection
	 * @param b_isJoining - Whether or not this is the master's first connection to an active master
	 * @return Worker for the connection
	 */
	public Worker addClient(DefaultSocketConnection sc_newClient, boolean b_isJoining)
	{
		Worker w_new = null;
		
		synchronized (arr_client) 
		{
			arr_client.add(sc_newClient);
		}
		synchronized (arr_worker) 
		{
			arr_worker.add(new Worker(sc_newClient, s_settings, l_main, b_isJoining, t_type, f_feeds, c_communication));
			es_workerPool.submit(arr_worker.get(arr_worker.size()-1));
			
			w_new = arr_worker.get(arr_worker.size()-1);
		}
		
		int i_runningWorkers = 0;
		
		for(int i_workerIndex = 0; i_workerIndex < arr_client.size(); i_workerIndex++)
		{
			if(arr_client.get(i_workerIndex).isConnected()) i_runningWorkers++;
		}
		
		lh_communication.write("Added; running: "+i_runningWorkers);
		
		return w_new;
	}
	
	/**Connects to a master
	 * @param mc_credentials - Credentials for master (location, port, key)
	 * @return MessageExecutor for the connection 
	 */
	public MessageExecutor connect(MasterCredentials mc_credentials)
	{
		DefaultSocketConnection sc_new = new DefaultSocketConnection((String)mc_credentials.get(MasterCredentials.Properties.location), Integer.valueOf((String)mc_credentials.get(MasterCredentials.Properties.masterPort)));
		
		sc_new.connect();
		
		if(sc_new.isConnected())
		{
			lh_communication.write("Connected to master "+mc_credentials.get(MasterCredentials.Properties.location));
			Worker w_new = addClient(sc_new, true);

			Message msg_key = new Message(Message.Type.Request, Value.Authorization);
			msg_key.addItem("key", (String)mc_credentials.get(Properties.key));

			w_new.mx_executor.send(msg_key);

			return w_new.mx_executor;
		}
		 else {
			 lh_communication.write(Entry.Type.Error, "Failed to connect to "+sc_new.getAddress());
		}
		
		return null;
	}
	
	/**Returns a mapping of the addresses and ports of the currently connected masters
	 * @return Map of the addresses to the ports
	 * @author murphyc1
	 */
	public Map<String, Integer> getConnectedMasters()
	{
		Map<String, Integer> map_masterAddresses = new HashMap<String, Integer>();
		
		for(int i_client = 0; i_client < arr_client.size(); i_client++)
		{
			if(!arr_client.get(i_client).isClosed())
			{
				String str_address = arr_client.get(i_client).getAddress();
				str_address = str_address.substring(1, str_address.indexOf(":"));
				
				map_masterAddresses.put(str_address, arr_client.get(i_client).getPort());
			}
			else
			{
				arr_client.remove(i_client);
				
				// Call again since we have modified the loop limits
				map_masterAddresses = getConnectedMasters();
				break; // Make sure we do not continue
			}
		}
		
		return map_masterAddresses;
	}
	
	/**Broadcasts a message to all connected masters
	 * @param msg_broadcast - Message to broadcast
	 * @author murphyc1
	 */
	public void broadcast(Message msg_broadcast)
	{
		lh_communication.write("Broadcasting to all other masters:\r\n"+msg_broadcast.toString());
		
		for(Worker w_worker : arr_worker)
		{
			w_worker.mx_executor.send(msg_broadcast);
		}
		
		lh_communication.write("Sent to "+arr_worker.size()+" masters");
	}
	
	/**Accessor for the Worker list
	 * @return All workers for the master
	 * @author murphyc1
	 */
	public ArrayList<Worker> getWorkers()
	{
		return arr_worker;
	}
	
	/**Accessor for the number of current connections to other masters
	 * @return Number of current connections
	 */
	public int getConnectionCount()
	{
		int i_connectionCount = 0;
		ArrayList<DefaultSocketConnection> arr_toBeRemoved = new ArrayList<DefaultSocketConnection>();
		
		for(int i_clientIndex = 0; i_clientIndex < arr_client.size(); i_clientIndex++)
		{
			if(!arr_client.get(i_clientIndex).isClosed())
			{
				i_connectionCount++;
			}
			else
			{
				arr_toBeRemoved.add(arr_client.get(i_clientIndex));
			}
		}
		
		if(!arr_client.removeAll(arr_toBeRemoved))
		{
			lh_communication.write(Entry.Type.Error, "Failed to remove all of closed connections from master's client array (Removal Queue: "+arr_toBeRemoved.size()+"; Total Size: "+(i_connectionCount + arr_toBeRemoved.size())+")");
		}
		
		return i_connectionCount;
	}
	
	public Communication getParent()
	{
		return c_communication;
	}
	
	/**Sends a message to a master at a specific address
	 * @param str_address Address of master
	 * @param msg_message Message to send
	 * @return True if the master is connected and exists
	 */
	public boolean sendTo(String str_address, Message msg_message)
	{
		boolean b_success = false;
		
		for(Worker w_worker : arr_worker)
		{
			if(w_worker.getClientAddress() == str_address)
			{
				w_worker.mx_executor.send(msg_message);
				
				b_success = true;
				break;
			}
		}
		
		return b_success;
	}
	
	/**Accessor for Feeds object 	
	 * @return The master's instance of the Feed object containing its feeds and the system's feed assignments
	 */
	public Feeds getFeeds()
	{
		return f_feeds;
	}
}