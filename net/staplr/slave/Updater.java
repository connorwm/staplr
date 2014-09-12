package net.staplr.slave;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import net.staplr.common.DatabaseAuth;
import net.staplr.common.Settings;
import net.staplr.common.feed.Entry;
import net.staplr.common.feed.FeedDocument;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;


/**Update thread to post FeedDocument and its entries to the database
 * @param FeedDocument document
 * @author murphyc1
 */
public class Updater implements Runnable
{
	// mongodb://<dbuser>:<dbpassword>@ds033757.mongolab.com:33757/staplr
	private Mongo mongoInstance;
	private DB staplrDB;
	private DBCollection feedCollection;
	
	private Settings settings;
	private Date lastSaveDate = null;
	private Date currentFeedDate = null;
	private FeedDocument fd_document;
	private UpdatableFeed uf_feed;
	
	
	public Updater(FeedDocument fd_document, UpdatableFeed uf_feed, Settings settings)
	{
		this.settings = settings;
		this.fd_document = fd_document;
		this.uf_feed = uf_feed;
		
		try {
			lastSaveDate = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss").parse((String)uf_feed.get(UpdatableFeed.Properties.lastSaveDate));
		} catch (ParseException excep_parse1) {
			excep_parse1.printStackTrace();
		} finally {
			try{
				String type = ((String)fd_document.get(FeedDocument.Properties.type));
				
				if(type.startsWith("RSS ")) // Use lastBuildDate
				{
					currentFeedDate = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss").parse((String)fd_document.get(FeedDocument.Properties.lastBuildDate));
				} 
				else if(type.startsWith("Atom ")) // Use pubDate
				{
					currentFeedDate = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss").parse((String)fd_document.get(FeedDocument.Properties.pubDate));
				}
			} catch (ParseException excep_parse2) {
				excep_parse2.printStackTrace();
			}
		}
	}
	
	public void run()
	{
		if(currentFeedDate.after(lastSaveDate) || lastSaveDate == null)
		{
			System.out.println("> Updating...");
			
			BasicDBObject dbo_document = new BasicDBObject();
			BasicDBObject dbo_entries = new BasicDBObject();
			ArrayList<Entry> e_entries = fd_document.getEntries();
				
			// Basic Document Properties
			for(int i_documentPropertyIndex = 0; i_documentPropertyIndex < FeedDocument.Properties.values().length; i_documentPropertyIndex++)
			{
				dbo_document.put((String)FeedDocument.Properties.values()[i_documentPropertyIndex].toString(), (String)fd_document.get(FeedDocument.Properties.values()[i_documentPropertyIndex]));
			}

			// Now add the entries
			for(int i_entryIndex = 0; i_entryIndex < e_entries.size(); i_entryIndex++)
			{
				BasicDBObject dbo_entry = new BasicDBObject();
				Entry e_entry = e_entries.get(i_entryIndex);

				ArrayList<String> str_categories = e_entry.getCategories();

				for(int i_entryPropertyIndex = 0; i_entryPropertyIndex < Entry.Properties.values().length; i_entryPropertyIndex++)
				{
					String value = e_entry.get(Entry.Properties.values()[i_entryPropertyIndex]);

					if(value != null)
					{
						dbo_entry.put(Entry.Properties.values()[i_entryPropertyIndex].toString(), value);
					}
				}

				for(int i_categoryIndex = 0; i_categoryIndex < str_categories.size(); i_categoryIndex++)
				{
					dbo_entry.put("category", str_categories.get(i_categoryIndex));
				}

				dbo_entries.put("entry", dbo_entry);
			}
		} else {
			System.out.println("> Up to date!");
		}
	}
}