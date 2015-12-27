package net.staplr.master;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;

import FFW.Utility.Error;
import FFW.Utility.ErrorProne;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import net.staplr.common.Settings;
import net.staplr.logging.Log;
import net.staplr.logging.LogHandle;
import net.staplr.common.DatabaseAuth.Properties;
import net.staplr.common.feed.Feed;
import net.staplr.logging.Entry;

public class DatabaseExecutor implements ErrorProne
{
	private Settings s_settings;
	private Error err_last;
	private LogHandle lh_dbx;
	private Logger l_mongo;
	
	public MongoDatabase db_feeds;
	public MongoDatabase db_entries;
	public MongoDatabase db_statistics;
	public MongoDatabase db_associations;
	
	private MongoClient mc_feeds;
	private MongoClient mc_entries;
	private MongoClient mc_statistics;
	private MongoClient mc_associations;
	
	private MongoClientOptions mco_options;
	
	public DatabaseExecutor(Settings s_settings, Log l_main)
	{
		this.s_settings = s_settings;
		err_last = null;
		
		lh_dbx = new LogHandle("dbx", l_main);
		
		// Setup the Mongo client options for each
		// - Select quickest connection
		// - Do not keep sockets alive (throw away and only build when needed)
		// - Max idle time of 60 seconds
		mco_options = MongoClientOptions.builder()
						.readPreference(ReadPreference.nearest())
						.socketKeepAlive(false)
						.maxConnectionIdleTime(60000)
						.build();
		
		// Quiet status messages that we are not concerned with
		// These logging messages will be displayed on startup for Settings due to this being defined afterwards
		// Leave this as such to ensure we have enough output to troubleshoot any database problems without modifying
		// the code and relaunching
		l_mongo = Logger.getLogger("org.mongodb.driver");
		l_mongo.setLevel(Level.SEVERE);
	}
	
	public boolean connect()
	{
		boolean b_success = false;
		lh_dbx.write("Connecting to databases...");
		
		//-------------------------------------------------------------------------------------------------------------
		// Feeds
		//-------------------------------------------------------------------------------------------------------------
		try
		{
			mc_feeds = new MongoClient(
					s_settings.map_databaseAuth.get("feeds").toServerAddress(), 
					Arrays.asList(s_settings.map_databaseAuth.get("feeds").toMongoCredential()),
					mco_options
					);
			
			db_feeds = mc_feeds.getDatabase("feeds");
		} 
		catch (Exception e) 
		{
			lh_dbx.write(Entry.Type.Error, "Failed to connect to feeds database");
			err_last = new Error("feeds", Error.Type.Initiation, e.toString());
		}
		finally 
		{
			// Unchecked finally blocks from here down
			lh_dbx.write("Connected to feeds database");			
			
		//-------------------------------------------------------------------------------------------------------------
		// Entries
		//-------------------------------------------------------------------------------------------------------------
			try
			{				
				mc_entries = new MongoClient(
						s_settings.map_databaseAuth.get("entries").toServerAddress(), 
						Arrays.asList(s_settings.map_databaseAuth.get("entries").toMongoCredential()),
						mco_options
						);
				
				db_entries = mc_entries.getDatabase("entries");
			}
			catch (Exception e) 
			{
				lh_dbx.write(Entry.Type.Error, "Failed to connect to entries database");
				err_last = new Error("entries", Error.Type.Initiation, e.toString());
			} 
			finally 
			{
				lh_dbx.write("Connected to entries database");
				
		//-------------------------------------------------------------------------------------------------------------
		// Statistics
		//-------------------------------------------------------------------------------------------------------------				
				try
				{					
					mc_statistics = new MongoClient(
							s_settings.map_databaseAuth.get("statistics").toServerAddress(), 
							Arrays.asList(s_settings.map_databaseAuth.get("statistics").toMongoCredential()),
							mco_options
							);
					
					db_statistics = mc_statistics.getDatabase("statistics");
				}
				catch (Exception e) 
				{
					lh_dbx.write(Entry.Type.Error, "Failed to connect to statistics database");
					err_last = new Error("statistics", Error.Type.Initiation, e.toString());
				} 
				finally 
				{
					lh_dbx.write("Connected to statistics database");
					
		//-------------------------------------------------------------------------------------------------------------
		// Associations
		//-------------------------------------------------------------------------------------------------------------
					try
					{
						mc_associations = new MongoClient(
								s_settings.map_databaseAuth.get("associations").toServerAddress(), 
								Arrays.asList(s_settings.map_databaseAuth.get("associations").toMongoCredential()),
								mco_options
								);
						
						db_associations = mc_associations.getDatabase("associations");
					} 
					catch (Exception e) 
					{
						lh_dbx.write(Entry.Type.Error, "Failed to connect to associations database");
						err_last = new Error("associations", Error.Type.Initiation, e.toString());
					} 
					finally 
					{
						lh_dbx.write("Connected to associations database");
						lh_dbx.write("Database startup successful");
						b_success = true;
					}
				}
				
			}
		}
		
		return b_success;
	}
	
	public ArrayList<Feed> getAllFeeds()
	{
		ArrayList<Feed> arr_feed = new ArrayList<Feed>();
		MongoCollection col_feed = db_statistics.getCollection("feeds");
		MongoCursor cur_feed = ((MongoIterable)col_feed.find()).iterator();
		
		while(cur_feed.hasNext())
		{
			Document doc_feed = (Document) cur_feed.next();
			Feed f_feed = new Feed();
			
			for(int i_propertyIndex = 0; i_propertyIndex < Feed.Properties.values().length; i_propertyIndex++)
			{
				if(doc_feed.containsKey(Feed.Properties.values()[i_propertyIndex].toString()))
				{
					f_feed.set(Feed.Properties.values()[i_propertyIndex], (String)doc_feed.get(Feed.Properties.values()[i_propertyIndex].toString()));
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