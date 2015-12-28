package net.staplr.common;

import java.util.ArrayList;

import FFW.Network.DefaultSocketConnection;

import net.staplr.common.Settings;
import net.staplr.logging.Entry;
import net.staplr.logging.Log;
import net.staplr.logging.LogHandle;
import net.staplr.master.Communication;
import net.staplr.master.Feeds;
import net.staplr.common.message.Message;
import net.staplr.common.message.MessageEnsurer;
import net.staplr.common.message.Message.Type;
import net.staplr.common.message.Message.Value;
import net.staplr.common.Communicator;

public class Worker implements Runnable
{
	private DefaultSocketConnection sc_client;
	private MessageEnsurer me_ensurer;
	public net.staplr.common.message.MessageExecutor mx_executor;
	private Thread t_ensurer;
	private Communication c_communication;
	
	private ArrayList<Message> msg_inbox;
	private ArrayList<Message> msg_outbox;
	
	private Communicator.Type t_type;
	private LogHandle lh_worker;
	private boolean b_run;
	private boolean b_disconnecting;
	
	public Worker(DefaultSocketConnection sc_client, Settings s_settings, Log l_main, 
			boolean b_isJoining, Communicator.Type t_type, Feeds f_feeds, Communication c_communication)
	{
		this.sc_client = sc_client;
		msg_inbox = new ArrayList<Message>();
		msg_outbox = new ArrayList<Message>();
		this.c_communication = c_communication; 
		
		b_run = true;
		b_disconnecting = false; // This variable is set when the client requests a Disconnect
								 // MessageExecutor will set this variable
		
		lh_worker = new LogHandle("wrkr", l_main);
		lh_worker.write("Working for client at "+sc_client.getAddress());
		
		// Setup MessageExecutor depending on type of Worker
		switch(t_type)
		{
		case Service:
			mx_executor = new net.staplr.service.MessageExecutor(sc_client, lh_worker, me_ensurer, msg_inbox, msg_outbox, s_settings, f_feeds, this);
			break;
		case Master:
			mx_executor = new net.staplr.master.MessageExecutor(sc_client, lh_worker, me_ensurer, msg_inbox, msg_outbox, s_settings, c_communication.getMasterCommunicator(), f_feeds, this);
			break;
		}
		
		this.t_type = t_type;
		
		
		// Initiate and start a MessageEnsurer for this Worker
		me_ensurer = new MessageEnsurer(sc_client, mx_executor, msg_inbox, msg_inbox, s_settings, lh_worker);
	
		t_ensurer = new Thread(me_ensurer);
		t_ensurer.start();
	}
	
	public void run()
	{
		Message msg_received = null;
		String str_received = new String();

		while(b_run)
		{			
			// TODO Fix ambiguation with being connected versus being closed in FFW library
			if(!sc_client.isClosed())
			{
				try
				{
					str_received = sc_client.receive();
				}
				catch (Exception excep_receive)
				{
					lh_worker.write(Entry.Type.Error, "Could not receive message:\r\n"+excep_receive.toString());					
				}
				finally
				{
					if(str_received != null)
					{
						lh_worker.write("Received: "+str_received);
						msg_received = new Message(str_received);
					}
					else
					{
						lh_worker.write(Entry.Type.Error, "Received null from client; disconnecting...");
						sc_client.close();
					}
					
					// If there was a message received and it was parsed then add it to the inbox
					if(msg_received != null && str_received != null)
					{
						msg_inbox.add(msg_received);
						
						// If the message is valid then we may execute it
						if(msg_received.isValid())
						{
							mx_executor.execute(msg_received);
						}
						else
						{							
							lh_worker.write(Entry.Type.Error, "Received invalid message:\r\n"+msg_received.toString());
						
							// If not then send an invalid message response
							Message msg_invalidMessage = new Message(Type.Response, Value.Invalid);
							msg_invalidMessage.addItem("message", msg_received.toString());
							
							mx_executor.send(msg_invalidMessage);
						}
					}
				}
			}
			else
			{
				lh_worker.write("Client no longer connected; shutting down...");
				b_run = false;
				
				// If a disconnect was unexpected (not requested by client) then handle accordingly
				if(!b_disconnecting)
				{
					// If this Worker was connected to another master then we need to verify it is down from other masters
					if(t_type == Communicator.Type.Master)
					{					
						c_communication.verifyDownMaster(getClientAddress());		
					}
				}
			}
		}
	}
	
	public String getClientAddress()
	{
		return sc_client.getAddress();
	}
	
	public void setExpectDisconnect()
	{
		b_disconnecting = true;
	}
}