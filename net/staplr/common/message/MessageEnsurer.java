package net.staplr.common.message;

import java.util.ArrayList;

import org.joda.time.DateTime;

import net.staplr.common.message.Message;
import net.staplr.common.message.Message.Type;
import net.staplr.common.message.Message.Value;
import net.staplr.common.Settings;
import net.staplr.logging.Log;
import net.staplr.logging.Log.Options;
import net.staplr.logging.LogHandle;
import FFW.Network.DefaultSocketConnection;

public class MessageEnsurer implements Runnable
{
	private boolean b_run;
	private DefaultSocketConnection sc_client;
	private LogHandle lh_worker;
	private MessageExecutor mx_executor;
	private ArrayList<Message> msg_inbox;
	private ArrayList<Message> msg_outbox;
	private Settings s_settings;
	
	public MessageEnsurer(DefaultSocketConnection sc_client, MessageExecutor mx_executor, ArrayList<Message> msg_inbox, ArrayList<Message> msg_outbox, Settings s_settings, LogHandle lh_worker)
	{
		this.sc_client = sc_client;
		this.mx_executor = mx_executor;
		this.msg_inbox = msg_inbox;
		this.msg_outbox = msg_outbox;
		this.s_settings = s_settings;
		this.lh_worker = lh_worker;
		
		b_run = true;
	}
	
	public void run()
	{
		ArrayList<Message> msg_removeQueue = new ArrayList<Message>();
		
		while(b_run)
		{
			try{
				// ------------------------------------------------------------
				// Inbox
				// ------------------------------------------------------------
				//System.out.println("inbox size: "+msg_inbox.size());
				for(int i_messageIndex = 0; i_messageIndex < msg_inbox.size(); i_messageIndex++)
				{
					Message msg_in = msg_inbox.get(i_messageIndex);
					
					if(msg_in.requiresResponse()) // TODO NullPointerException in event of disconnection
					{
						if(msg_in.respondedTo())
						{
							lh_worker.write("Removing responded to received message:\r\n"+msg_in);
							msg_removeQueue.add(msg_in);
						}
					} 
					else 
					{
						DateTime dt_timeSent = msg_in.getTimeReceived();
						
						if(dt_timeSent.plusMinutes(1).isBeforeNow())
						{
							lh_worker.write("Removing old received message:\r\n"+msg_in);
							msg_removeQueue.add(msg_in);
						}
					}
				}
				
				if(msg_removeQueue.size() > 0)	msg_inbox.removeAll(msg_removeQueue);
				
				// ------------------------------------------------------------
				// Outbox
				// ------------------------------------------------------------
				for(int i_messageIndex = 0; i_messageIndex < msg_outbox.size(); i_messageIndex++)
				{
					Message msg_out = msg_outbox.get(i_messageIndex);
					
					if(msg_out.getTimeSent() != null)
					{
						if(msg_out.isDelivered())
						{
							if(msg_out.requiresResponse())
							{
								if(!msg_out.hasReceivedResponse())
								{
									DateTime dt_timeSent = msg_out.getTimeSent();
									
									if(dt_timeSent.plusMinutes(1).isBeforeNow())
									{
										lh_worker.write("Message missing required response:\r\n"+msg_out);
										
										Message msg_responseRequest = new Message(Type.Request, Value.ResponseRequest);
										msg_responseRequest.addItem("message", msg_out.toString());
										
										mx_executor.send(msg_responseRequest);
									}
								} 
								else
								{
									lh_worker.write("Removing old sent message:\r\n"+msg_out);
									msg_removeQueue.add(msg_out);
								}
							} 
							else 
							{
								lh_worker.write("Removing old sent message:\r\n"+msg_out);
								msg_removeQueue.add(msg_out);
							}
						} 
						else 
						{
							DateTime dt_timeSent = msg_out.getTimeSent();
							
							if(dt_timeSent.plusMinutes(1).isBeforeNow())
							{
								lh_worker.write("Message delivery notice missing:\r\n"+msg_out);
								lh_worker.write("Sending delivery request");
								
								Message msg_deliveryNoticeRequest = new Message(Type.Request, Value.DeliveryNoticeRequest);
								msg_deliveryNoticeRequest.addItem("message", msg_out.toString());
								
								mx_executor.send(msg_deliveryNoticeRequest);
							}
						}
					} else {
						lh_worker.write("Removing sent message with no time sent:\r\n"+msg_out);
						msg_removeQueue.add(msg_out);
					}
				}
				
				if(msg_removeQueue.size() > 0)	msg_outbox.removeAll(msg_removeQueue);
				
				// Sleep time bitch!
				try{
					Thread.sleep(15000);
				} catch (Exception e) {}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void stop()
	{
		b_run = false;
	}
}