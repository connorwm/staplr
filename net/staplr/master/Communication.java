package net.staplr.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.staplr.common.Communicator;
import net.staplr.common.Settings;
import net.staplr.common.MasterCredentials.Properties;
import net.staplr.common.Worker;
import net.staplr.common.message.Message;
import net.staplr.common.message.MessageExecutor;
import net.staplr.logging.Entry;
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
	private Map<String, FeedRedistribution> map_feedRedistribution;
	
	public Communication(Settings s_settings, int i_servicePort, int i_masterCommunicationPort, Log l_main, Feeds f_feeds)
	{
		this.s_settings = s_settings;
		
		lh_communication = new LogHandle("com", l_main);
		
		c_service = new Communicator(this, s_settings, i_servicePort, Communicator.Type.Service, l_main, f_feeds);
		c_master = new Communicator(this, s_settings, i_masterCommunicationPort, Communicator.Type.Master, l_main, f_feeds);
		es_components = Executors.newCachedThreadPool();
		
		arr_connectionChecks = new ArrayList<ConnectionCheck>();
		map_feedRedistribution = new HashMap<String, FeedRedistribution>();
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
	
	/**Sends a message to a fellow master to check and confirm if down
	 * @param str_address - Address of master to confirm if down
	 */
	public void verifyDownMaster(String str_address)
	{
		lh_communication.write("Messaging other masters to confirm if master at "+str_address+" is down");
		
		if(c_master.getConnectedMasters().keySet().size() == 0)
		{
			Message m_connectionCheck = new Message(Type.Request, Value.ConnectionCheck);
			m_connectionCheck.addItem("address", str_address);
			
			c_master.broadcast(m_connectionCheck);
			
			ConnectionCheck cc_check = new ConnectionCheck(str_address, (String[])c_master.getConnectedMasters().keySet().toArray());
			arr_connectionChecks.add(cc_check);
		}
		else
		{
			lh_communication.write("\tNo other masters; redistributing feeds now");
			redistributeFeeds(str_address);
		}
	}
	
	/**Polls masters from database to find one to sync with on startup. Function will loop through the list until an available master is found.
	 * @return Boolean result as to whether or not joining another master was successful 
	 */
	public boolean joinMasters()
	{
		MessageExecutor mx_executor = null;
		Message msg_sync = new Message(Message.Type.Request, Message.Value.Sync);
		lh_communication.write("Attempting to join masters...");
		boolean b_result = false;
		
		for(int i_masterIndex = 0; i_masterIndex < s_settings.mc_credentials.size(); i_masterIndex++)
		{
			lh_communication.write("Connecting to "+s_settings.mc_credentials.get(i_masterIndex).get(Properties.location));
			mx_executor = c_master.connect(s_settings.mc_credentials.get(i_masterIndex));
			
			if(mx_executor != null)
			{
				lh_communication.write("\tConnection established; sending sync request");
				mx_executor.send(msg_sync);
				b_result = true;
				
				break;
			}
		}
		
		return b_result;
	}
	
	/**Remove worker after connection has been closed. All follow up for close will be handled separately.
	 * @param w_worker
	 */
	public void cleanupClosedConnection(Worker w_worker)
	{
		ArrayList<Worker> arr_workers = null;
		
		switch(w_worker.getType())
		{
		case Master:
			arr_workers = c_master.getWorkers();
			break;
			
		case Service:
			arr_workers = c_service.getWorkers();
			break;
		};
		
		if(arr_workers != null)
		{
			for(int i_workerIndex = 0; i_workerIndex < arr_workers.size(); i_workerIndex++)
			{
				// Lookup worker to remove in array by its address
				if(arr_workers.get(i_workerIndex).getClientAddress() == w_worker.getClientAddress())
				{
					arr_workers.remove(i_workerIndex);
					lh_communication.write("Successfully removed "+w_worker.getType()+" worker for "+w_worker.getClientAddress()+"");
					
					break;
				}
			}
		}
		else
		{
			lh_communication.write(Entry.Type.Error, "Unknown Worker type encountered while trying to remove: '"+w_worker.getType().toString()+"'");
		}
	}
	
	/**Initiates the redistribute feeds process by sending out its redistribute number or handling the redistribution
	 * if only one master is connected
	 * @author murphyc1
	 */
	public void redistributeFeeds(String str_address)
	{
		lh_communication.write("Redistributing Feeds...");
		
		if(c_master.getConnectionCount() == 0)
		{
			lh_communication.write("No other masters connected; redistributing feeds to self");
			
			Map<String, ArrayList<String>> map_assignments = c_master.getFeeds().getAssignments();
			Iterator<ArrayList<String>> it_assignments = map_assignments.values().iterator();
			
			for(int i_assignmentIndex = 0; i_assignmentIndex < map_assignments.values().size(); i_assignmentIndex++)
			{
				ArrayList<String> arr_feeds = it_assignments.next();
				
				if(map_assignments.keySet().toArray()[i_assignmentIndex] != "127.0.0.1")
				{
					if(!map_assignments.get("127.0.0.1").addAll(arr_feeds))
					{
						lh_communication.write(Entry.Type.Error, "Failed to add all of the feeds from "+(String)map_assignments.keySet().toArray()[i_assignmentIndex]);
					}
					else
					{
						lh_communication.write("Added "+arr_feeds.size()+" feeds from "+map_assignments.keySet().toArray()[i_assignmentIndex]+" to self");
					}
				}
			}
			
			lh_communication.write("Finished distributing feeds to self");
		}
		else
		{
			sendRedistributeNumber(str_address);
		}
	}
	
	/**Sends out a redistribution number (randomly selected number) for choosing a master to redistribute the feeds
	 * @param str_address Address of downed master we are setting up a redistribution for
	 * @author murphyc1
	 */
	public void sendRedistributeNumber(String str_address)
	{
		lh_communication.write("Redistributing Feeds: sending redistribute number to other masters");
		
		/*
		 * Decentralized Feed Redistribution
		 * 
		 * Pick a random number and send it to all of the other masters
		 * Rechoose and resend a number if someone else has the same one
		 * The master with the highest number redistributes the feeds
		 */
		
		Message msg_redistributeNumber = new Message(Type.Request, Value.RedistributeNumber);
		msg_redistributeNumber.addItem("number", String.valueOf(new Random().nextInt(65535)));
		
		c_master.broadcast(msg_redistributeNumber);
		map_feedRedistribution.get(str_address).getRedistributeNumbers().put("127.0.0.1", Integer.valueOf((String)msg_redistributeNumber.get("number")));
	}
	
	public Communicator getMasterCommunicator()
	{
		return c_master;
	}
	
	public Communicator getServiceCommunicator()
	{
		return c_service;
	}
	
	public Map<String, FeedRedistribution> getFeedRedistributionMap()
	{
		return map_feedRedistribution;
	}
}