package net.staplr.master;

import java.util.ArrayList;

import FFW.Utility.Error;
import FFW.Utility.Error.Type;
import FFW.Utility.ErrorProne;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import net.staplr.common.DatabaseAuth;
import net.staplr.common.Settings;
import net.staplr.logging.Log;
import net.staplr.logging.Log.Options;
import net.staplr.logging.LogHandle;
import net.staplr.slave.Slave;
import net.staplr.common.feed.Feed;

public class DatabaseExecutor implements ErrorProne
{
	private Settings s_settings;
	private Error err_last;
	private LogHandle lh_dbx;
	
	public DB db_feeds;
	public DB db_entries;
	public DB db_statistics;
	public DB db_associations;
	
	private Mongo m_feeds;
	private Mongo m_entries;
	private Mongo m_statistics;
	private Mongo m_associations;
	
	public DatabaseExecutor(Settings s_settings, Log l_main)
	{
		this.s_settings = s_settings;
		err_last = null;
		
		lh_dbx = new LogHandle("dbx", l_main);
	}
	
	public boolean connect()
	{
		lh_dbx.write("Connecting to databases...");
		
		try
		{
			m_feeds = new Mongo((String)s_settings.map_databaseAuth.get("feeds").get(DatabaseAuth.Properties.location), Integer.parseInt((String)s_settings.map_databaseAuth.get("feeds").get(DatabaseAuth.Properties.port)));
			db_feeds = m_feeds.getDB((String)s_settings.map_databaseAuth.get("feeds").get(DatabaseAuth.Properties.database));
		} 
		catch (Exception e) 
		{
			lh_dbx.write("Failed to connect to feeds database");
			err_last = new Error("feeds", Error.Type.Initiation, e.toString());
		}
		finally 
		{
			lh_dbx.write("Connected to feeds database");
			
			try
			{
				m_entries = new Mongo((String)s_settings.map_databaseAuth.get("entries").get(DatabaseAuth.Properties.location), Integer.parseInt((String)s_settings.map_databaseAuth.get("entries").get(DatabaseAuth.Properties.port)));
				db_entries = m_entries.getDB((String)s_settings.map_databaseAuth.get("entries").get(DatabaseAuth.Properties.database));
			}
			catch (Exception e) 
			{
				lh_dbx.write("Failed to connect to entries database");
				err_last = new Error("entries", Error.Type.Initiation, e.toString());
			} 
			finally 
			{
				lh_dbx.write("Connected to entries database");
				
				try
				{
					m_statistics = new Mongo((String)s_settings.map_databaseAuth.get("statistics").get(DatabaseAuth.Properties.location), Integer.parseInt((String)s_settings.map_databaseAuth.get("statistics").get(DatabaseAuth.Properties.port)));
					db_statistics = m_statistics.getDB((String)s_settings.map_databaseAuth.get("statistics").get(DatabaseAuth.Properties.database));
				}
				catch (Exception e) 
				{
					lh_dbx.write("Failed to connect to statistics database");
					err_last = new Error("statistics", Error.Type.Initiation, e.toString());
				} 
				finally 
				{
					lh_dbx.write("Connected to statistics database");
					
					try
					{
						m_associations = new Mongo((String)s_settings.map_databaseAuth.get("associations").get(DatabaseAuth.Properties.location), Integer.parseInt((String)s_settings.map_databaseAuth.get("associations").get(DatabaseAuth.Properties.port)));
						db_associations = m_associations.getDB((String)s_settings.map_databaseAuth.get("associations").get(DatabaseAuth.Properties.database));
					} 
					catch (Exception e) 
					{
						lh_dbx.write("Failed to connect to associations database");
						err_last = new Error("associations", Error.Type.Initiation, e.toString());
					} 
					finally 
					{
						lh_dbx.write("Connected to associations database");
						
						lh_dbx.write("Authenticating...");
						
						if(db_feeds.authenticate((String)s_settings.map_databaseAuth.get("feeds").get(DatabaseAuth.Properties.username), ((String)s_settings.map_databaseAuth.get("feeds").get(DatabaseAuth.Properties.password)).toCharArray())) 
						{
							lh_dbx.write("Authenticated feeds");
							
							if (db_entries.authenticate((String)s_settings.map_databaseAuth.get("entries").get(DatabaseAuth.Properties.username), ((String)s_settings.map_databaseAuth.get("entries").get(DatabaseAuth.Properties.password)).toCharArray()))
							{
								lh_dbx.write("Authenticated entries");
								
								if(db_statistics.authenticate(
										(String)s_settings.map_databaseAuth.get("statistics").get(DatabaseAuth.Properties.username), 
										((String)s_settings.map_databaseAuth.get("statistics").get(DatabaseAuth.Properties.password)).toCharArray()))
								{
									lh_dbx.write("Authenticated statistics");
									
									if(db_associations.authenticate(
											(String)s_settings.map_databaseAuth.get("associations").get(DatabaseAuth.Properties.username), 
											((String)s_settings.map_databaseAuth.get("associations").get(DatabaseAuth.Properties.password)).toCharArray()))
									{
										lh_dbx.write("Authenticated associations");
										lh_dbx.write("Database startup successful");
										
										return true;
									}
									else 
									{
										lh_dbx.write("Failed to authorize associations");
										err_last = new Error("db_entries", Type.Accept, db_entries.getLastError().toString());
									}
								}
								else 
								{
									lh_dbx.write("Failed to authorize statistics");
									err_last = new Error("db_entries", Type.Accept, db_entries.getLastError().toString());
								}
							} 
							else 
							{
								lh_dbx.write("Failed to authorize entries");
								err_last = new Error("db_entries", Type.Accept, db_entries.getLastError().toString());
							}
						} 
						else 
						{
							lh_dbx.write("Failed to authorize feeds");
							err_last = new Error("db_feeds", Type.Accept, db_feeds.getLastError().toString());
						}
					}
				}
				
			}
		}
		
		// Close since we won't be using them :(
		m_feeds.close();
		m_entries.close();
		
		return false;
	}
	
	public ArrayList<Feed> getAllFeeds()
	{
		ArrayList<Feed> arr_feed = new ArrayList<Feed>();
		DBCollection col_feed = db_statistics.getCollectionFromString("feeds");
		DBCursor cur_feed = col_feed.find();
		
		while(cur_feed.hasNext())
		{
			DBObject dbo_feed = cur_feed.next();
			Feed f_feed = new Feed();
			
			for(int i_propertyIndex = 0; i_propertyIndex < Feed.Properties.values().length; i_propertyIndex++)
			{
				if(dbo_feed.containsField(Feed.Properties.values()[i_propertyIndex].toString()))
				{
					f_feed.set(Feed.Properties.values()[i_propertyIndex], (String)dbo_feed.get(Feed.Properties.values()[i_propertyIndex].toString()));
				}
			}
			
			arr_feed.add(f_feed);
		}
		
		return arr_feed;
	}

	public Error getLastError() {
		return err_last;
	}
	
	
}