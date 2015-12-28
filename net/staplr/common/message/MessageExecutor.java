package net.staplr.common.message;

import net.staplr.logging.LogHandle;

import java.util.ArrayList;

import FFW.Network.DefaultSocketConnection;
import net.staplr.common.Settings;
import net.staplr.common.Worker;
import net.staplr.logging.Entry;
import net.staplr.logging.Log;
import net.staplr.common.Settings.Setting;
import net.staplr.common.message.Message.Type;
import net.staplr.common.message.Message.Value;

public class MessageExecutor
{
	private DefaultSocketConnection sc_client;
	private Settings s_settings;
	private LogHandle lh_worker;
	private Worker w_worker;
	private ArrayList<Message> msg_inbox;
	public ArrayList<Message> msg_outbox; // TODO why is this public?
	
	public MessageExecutor(DefaultSocketConnection sc_client, LogHandle lh_worker, MessageEnsurer me_ensurer, ArrayList<Message> msg_inbox, ArrayList<Message> msg_outbox, Settings s_settings, Worker w_worker)
	{
		this.sc_client = sc_client;
		this.lh_worker = lh_worker;
		this.msg_inbox = msg_inbox;
		this.msg_outbox = msg_outbox;
		this.s_settings = s_settings;
		this.w_worker = w_worker;
	}
	
	public void execute(Message msg_message)
	{
		// Send a delivery notice to the sender if necessary
		if(msg_message.getValue() == Value.DeliveryNotice)
		{
			Message msg_delivered = null;
			
			try
			{
				msg_delivered = new Message(String.valueOf(msg_message.get("message")));
			} 
			catch (Exception e)
			{
				respondInvalid(msg_message);
			}
			finally
			{
				lh_worker.write("Looking in outbox for "+msg_delivered);
				boolean b_found = false;
				Message msg_original = null;
				
				for(int i_messageIndex = 0; i_messageIndex < msg_outbox.size(); i_messageIndex++)
				{
					Message msg_current = msg_outbox.get(i_messageIndex);
					
					if(msg_current.getType() == msg_delivered.getType() && msg_current.getValue() == msg_delivered.getValue())
					{
						if(msg_current.toString().equals(msg_delivered.toString()))
						{
							msg_original = msg_current;
							b_found = true;
							break;
						}
					}
				}
				
				if(b_found)
				{
					lh_worker.write("Received Delivery Notice for:\r\n"+msg_delivered.toString());

					msg_original.setDelivered();
				} else {
					lh_worker.write(Entry.Type.Error, "Delivery Notice:\r\n"+msg_delivered.toString()+"Does not have a matching message");
					lh_worker.write("Current Outbox Messages:\r\n"+msg_outbox.toString());
				}
			}
		} 
		else if (msg_message.getValue() == Value.DeliveryNoticeRequest)
		{
			Message msg_requestedMessage = null;
			
			lh_worker.write("Received Delivery Notice Request");
			
			if(msg_message.get("message") != null)
			{
				msg_requestedMessage = new Message(String.valueOf(msg_message.get("message")));
				lh_worker.write("Looking for message:\r\n"+msg_requestedMessage.toString());
				
				if(msg_requestedMessage.isValid())
				{
					if(msg_inbox.contains(msg_requestedMessage))
					{
						// Send back a Delivery Notice
						Message msg_deliveryNotice = new Message(Type.Response, Value.DeliveryNotice);
						msg_deliveryNotice.addItem("message", msg_requestedMessage.toString());
						
						send(msg_deliveryNotice);
					} else {
						Message msg_neverReceived = new Message(Type.Response, Value.NeverReceived);
						msg_neverReceived.addItem("message", msg_requestedMessage.toString());
						
						send(msg_neverReceived);
						execute(msg_requestedMessage); 	// Now execute it since the DeliveryNoticeRequest
														// contained the message we didn't have
					}
				}
				else
				{
					respondInvalid(msg_message);
				}
			}
		}
		else
		{
			Message msg_deliveryNotice = new Message(Type.Response, Value.DeliveryNotice);
			msg_deliveryNotice.addItem("message", msg_message.toString());
			
			send(msg_deliveryNotice);
			
			// ----------------------------------------------
			// Message Processing
			// ----------------------------------------------
			
			if(msg_message.getType() == Type.Request)
			{
				if(msg_message.getValue() == Value.Authorization)
				{
					String str_key = (String)msg_message.get("key");
					lh_worker.write("Received Key: "+str_key);
					
					if(str_key.equals(String.valueOf(s_settings.get(Settings.Setting.masterKey))))
					{
						lh_worker.write("Key Accepted");
						
						Message msg_accepted = new Message(Type.Response, Value.Accepted);
						send(msg_accepted);
						
						//msg_message.setRespondedTo();
					}
					else
					{
						lh_worker.write("Key Rejected");
						
						Message msg_rejected = new Message(Type.Response, Value.Rejected);
						send(msg_rejected);
						
						//msg_message.setRespondedTo();
					}
				}
				else
				{
					executeAsPer(msg_message);
				}
			}
			else if (msg_message.getType() == Type.Response) 
			{
				if(msg_message.getValue() == Value.Accepted)
				{
					lh_worker.write("Accepted!");
				}
				else
				{
					executeAsPer(msg_message);
				}
			}
		}
	}
	
	protected void respondInvalid(Message msg_received)
	{
		Message msg_invalidMessage = new Message(Type.Response, Value.Invalid);
		msg_invalidMessage.addItem("message", msg_received.toString());
		
		send(msg_invalidMessage);
	}
	
	public synchronized void send(Message msg_message)
	{
		if(sc_client.send(msg_message.toString()))
		{
			lh_worker.write("Sent: "+msg_message.toString());
			
			msg_message.markAsSent();
			msg_outbox.add(msg_message);
		} else {
			lh_worker.write(Entry.Type.Error, "Failed to send: "+msg_message.toString()+"\r\n"+sc_client.getLastError().toString());
		}
	}
	
	public void executeAsPer(Message msg_message)
	{ 
		// Place holder for polymorphism
	}
}