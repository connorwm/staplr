package net.staplr.common;

import java.util.ArrayList;

import net.staplr.common.Settings;
import net.staplr.logging.Log;
import net.staplr.logging.LogHandle;
import net.staplr.master.Feeds;
import net.staplr.master.Master;
import FFW.Network.DefaultSocketConnection;
import net.staplr.common.message.Message;
import net.staplr.common.message.MessageEnsurer;
import net.staplr.common.message.Message.Type;
import net.staplr.common.message.Message.Value;
import net.staplr.common.Communicator;

public class Worker implements Runnable
{
	private DefaultSocketConnection sc_client;
	private Master master;
	private MessageEnsurer me_ensurer;
	public net.staplr.common.message.MessageExecutor mx_executor;
	private Thread t_ensurer;
	private Communicator c_communicator;
	
	private ArrayList<Message> msg_inbox;
	private ArrayList<Message> msg_outbox;
	
	private Communicator.Type t_type;
	private boolean b_run;
	private LogHandle lh_worker;
	
	public Worker(DefaultSocketConnection sc_client, Settings s_settings, Log l_main, 
			boolean b_isJoining, Communicator.Type t_type, Feeds f_feeds, Communicator c_communicator)
	{
		this.sc_client = sc_client;
		msg_inbox = new ArrayList<Message>();
		msg_outbox = new ArrayList<Message>();
		this.c_communicator = c_communicator; 
		
		b_run = true;
		
		lh_worker = new LogHandle("wrkr", l_main);
		lh_worker.write("Working for client at "+sc_client.getAddress());
		
		switch(t_type)
		{
		case Service:
			mx_executor = new net.staplr.service.MessageExecutor(sc_client, lh_worker, me_ensurer, msg_inbox, msg_outbox, s_settings, f_feeds);
			break;
		case Master:
			mx_executor = new net.staplr.master.MessageExecutor(sc_client, lh_worker, me_ensurer, msg_inbox, msg_outbox, s_settings, c_communicator, f_feeds);
			break;
		}
		
		this.t_type = t_type;
		
		me_ensurer = new MessageEnsurer(sc_client, mx_executor, msg_inbox, msg_inbox, s_settings, lh_worker);
		
	
		t_ensurer = new Thread(me_ensurer);
		t_ensurer.start();
	}
	
	public void run()
	{
		Message msg_received = null;
		String str_received = new String();
		// ----------------------------------------------------
		// Main Loop
		// ----------------------------------------------------

		while(b_run)
		{			
			if(sc_client.isConnected())
			{
				try
				{
					str_received = sc_client.receive();
					lh_worker.write("Received: "+str_received);
					msg_received = new Message(str_received);
				}
				catch (Exception e)
				{
					lh_worker.write("ERROR: Could not receive message:\r\n"+e.toString());
					
					if(str_received == null)
					{
						lh_worker.write("NOTICE: Assumed to be disconnected; ending communication processing");
						b_run = false;
						
						if(t_type == Communicator.Type.Master)
						{
							c_communicator.redistributeFeeds(sc_client.getAddress());
						}	
					}
				}
				finally
				{
					msg_inbox.add(msg_received);
					
					if(msg_received.isValid())
					{
						mx_executor.execute(msg_received);
					}
					else 
					{
						lh_worker.write("Received invalid message:\r\n"+msg_received.toString());
						
						Message msg_invalidMessage = new Message(Type.Response, Value.Invalid);
						msg_invalidMessage.addItem("message", msg_received.toString());
						
						mx_executor.send(msg_invalidMessage);
					}
				}
			}
			else
			{
				lh_worker.write("Client no longer connected; shutting down...");
				b_run = false;
				
				if(t_type == Communicator.Type.Master)
				{
					c_communicator.redistributeFeeds(sc_client.getAddress());
				}	
			}
		}
	}
	
	public String getClientAddress()
	{
		return sc_client.getAddress();
	}
}