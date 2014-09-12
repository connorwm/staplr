package net.staplr.slave;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import net.staplr.common.Settings;
import net.staplr.common.feed.Author;
import net.staplr.common.feed.Entry;
import net.staplr.common.feed.Feed;
import net.staplr.common.feed.FeedDocument;
import net.staplr.common.feed.Link;
import net.staplr.common.feed.Link.Properties;
import net.staplr.logging.Log;
import net.staplr.logging.LogHandle;
import net.staplr.master.DatabaseExecutor;
import net.staplr.processing.Processor;

import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.DB;

/**Essential runnable object for updating and parsing a feed
 * @author connorwm
 */
public class Slave implements Runnable
{
	private FeedParser feedParser;
	
	private DB db_feed;
	private DB db_entries;
	private DB db_statistics;
	
	private Settings s_settings;
	private Feed f_feed;
	private LogHandle lh_slave;
	private DBObject dbo_feedStatistics;
	private DBCollection col_feedStatistics;
	private Processor p_processor;
	private Log l_main;
	private DatabaseExecutor dx_executor;
	
	private Future<?> ftr_future;
	
	public Slave(Feed f_feed, DatabaseExecutor dx_executor, Settings s_settings, Log l_main)
	{
		this.f_feed = f_feed;
		this.s_settings = s_settings;
		this.dx_executor = dx_executor;
		this.db_feed = dx_executor.db_feeds;
		this.db_entries = dx_executor.db_entries;
		this.db_statistics = dx_executor.db_statistics;
		this.l_main = l_main;
		
		lh_slave = new LogHandle("slv", l_main);
		
	}
	
	public void run()
	{					
		lh_slave.write("Slave working on "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));

		// Load Up Feed's Statistics
		col_feedStatistics = db_statistics.getCollection("feeds");
		dbo_feedStatistics = getStatisticsDocument(f_feed, col_feedStatistics);	
		
		// Get the collections because we will need their documents
		DBCollection col_entries = db_entries.getCollection(f_feed.get(Feed.Properties.collection));
		DBCollection col_feeds = db_feed.getCollection(f_feed.get(Feed.Properties.collection));
		DBObject dbo_feed = getFeedDocument(f_feed, col_feeds);
		
		// Check entries collection
		if(!db_entries.collectionExists(f_feed.get(Feed.Properties.collection)))
		{
			lh_slave.write("Entries collection "+f_feed.get(Feed.Properties.collection)+" does not exist.");
		} else {		
			lh_slave.write("Entries from "+f_feed.get(Feed.Properties.collection)+" contains "+col_entries.count());
		}
		
		// Check feeds collection
		if(!db_feed.collectionExists(f_feed.get(Feed.Properties.collection)))
		{
			lh_slave.write("Feed collection "+f_feed.get(Feed.Properties.collection)+" does not exist."); 
		} else {
			lh_slave.write("Feeds from "+f_feed.get(Feed.Properties.collection)+" contains "+col_feeds.count());
		}
		
		// Now lets download the XML feed
		if(f_feed.get(Feed.Properties.url) != null)
		{
			// We'll need to know the date format
			String str_feedDateFormat = String.valueOf(f_feed.get(Feed.Properties.dateFormat));
			Downloader feedDownloader = new Downloader(f_feed.get(Feed.Properties.url), s_settings, str_feedDateFormat);

			// Now run it
			if(!feedDownloader.run())
			{
				// Couldn't be downloaded
				lh_slave.write("Could not download feed: "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+". Last Error: \r\n"+feedDownloader.getLastError().toString());
			} else {			
				//--------------------------------------------------------------------------
				// Parse feed document
				//--------------------------------------------------------------------------
				
				// It was downloaded so lets convert it into a FeedDocument to work with
				lh_slave.write("Downloaded "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
				FeedDocument fd_feedDocument = feedDownloader.getFeedDocument();
				
				lh_slave.write("Parsing "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"...");
				feedParser = new FeedParser(fd_feedDocument, f_feed, dbo_feed, lh_slave);
				feedParser.parse();
				lh_slave.write("Parsed "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+":\t"+dbo_feed.toString());
				
				lh_slave.write("Posting updated feed document of "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
				WriteResult updateResult = col_feeds.update(new BasicDBObject("_id", dbo_feed.get("_id")), dbo_feed, true, false);
				
				if(updateResult.getError() == null) 
				{
					lh_slave.write("Successfully updated "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
				}
				else 
				{
					lh_slave.write("Failed to update feed document of "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"; Last Error:\r\n"+updateResult.getError());
				}
				
				//--------------------------------------------------------------------------
				// Work on entries
				//--------------------------------------------------------------------------
				
				lh_slave.write("Working on entries for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+":");

				DBObject dbo_entry = null;
				ArrayList<DBObject> arr_entries = new ArrayList<DBObject>();
				ArrayList<Entry> entries = fd_feedDocument.getEntries();
				Entry e_entry = null;
				boolean collectionEmpty = false;
				DateTimeFormatter dtf = DateTimeFormat.forPattern((String)f_feed.get(Feed.Properties.dateFormat));

				// Check if collection is empty
				if(col_entries.count() > 0)
				{
					// Check if there are entries but they're just not from this feed
					if(findMostRecentEntry(String.valueOf(f_feed.get(Feed.Properties.name)), col_entries) == null)
					{
						lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" does not have entries, collection only from other feeds");
						collectionEmpty = true;
					}
					else
					{
						lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" already has entries");
					}
				} else {
					lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" does not have entries");
					collectionEmpty = true;
				}
				
				// Build entries
				lh_slave.write("Building entry list for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
				arr_entries = buildEntryList(col_feeds, collectionEmpty, entries);
				lh_slave.write("Built entry list for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
				
				//  If dbo_entries has entries, then we need to figure out which ones need to be posted
				if(arr_entries.size() > 0)
				{
					lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" has entries to be posted. "+arr_entries.size()+" to be parsed.");
					
					if(collectionEmpty)
					{
						lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" has no entries; posting all");
					}
					else
					{
						if(dbo_feedStatistics != null)
						{
							lh_slave.write("Have statistics document for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
							
							if(dbo_feedStatistics.get("timestamp") != null)
							{
								lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" has most recent date; removing entries before...");
								arr_entries = removeEntriesBefore(Long.parseLong(String.valueOf(dbo_feedStatistics.get("timestamp"))), arr_entries);
								lh_slave.write("Removed old entries from entries to be posted for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
							}
							else
							{
								lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" does not have a most recent date. Removing old entries...");
								arr_entries = removeOldEntries(arr_entries, f_feed, col_entries);
								lh_slave.write("Removed old entries from "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
							}
						}
						else
						{
							lh_slave.write("No statistics document for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+". Removing old entries...");
							arr_entries = removeOldEntries(arr_entries, f_feed, col_entries);
							lh_slave.write("Removed old entries from "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
						}
					}
					
					postEntries(arr_entries, col_entries);
					processEntries(arr_entries);
				}
				else
				{
					lh_slave.write("No entries to be posted for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
				}
			}
		} else {
			lh_slave.write("\tError: URL is null");
		}
		
		System.gc();
	}
	
	/**Removes all entries in the entries array that are older than the most recent entry in the specified feed
	 * @author connorwm
	 * @param dbo_entries - Array of entries that contains old entries
	 * @param f_feed - The corresponding feed
	 * @param col_entries - The entry collection corresponding to the feed
	 * @return DBObject array with old entries removed
	 */
	private ArrayList<DBObject> removeOldEntries(ArrayList<DBObject> dbo_entries, Feed f_feed, DBCollection col_entries)
	{
		DBObject dbo_mostRecentEntry = findMostRecentEntry(f_feed.get(Feed.Properties.name), col_entries);
		
		if(dbo_mostRecentEntry != null)
		{
			lh_slave.write("Found a most recent entry for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"; removing others before...");
			dbo_entries = removeEntriesBefore(Long.valueOf(String.valueOf(dbo_mostRecentEntry.get("timestamp"))), dbo_entries);
			lh_slave.write("Old entries removed from "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"; ready to post");
		}
		else
		{
			lh_slave.write("Could not find a most recent entry for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"; all will be posted");
		}
		
		return dbo_entries;
	}
	
	/**Finds the most recent entry in the the feed specified by str_feedName in the feed's collection
	 * @author connorwm
	 * @param str_feedName
	 * @param col_entryCollection
	 * @return DBObject with the most recent entry
	 */
	private DBObject findMostRecentEntry(String str_feedName, DBCollection col_entryCollection)
	{
		DBObject dbo_entry = new BasicDBObject();
		DBCursor cur_feedResults = null;
		
		// Find all entries in the collection for that collection's individual feed
		// Example: Gizmodo
		// We want to find the entries from the 'top' feed of Gizmodo. So set name = 'top';
		dbo_entry.put("feed", str_feedName);
		
		cur_feedResults = col_entryCollection.find(dbo_entry);
		
		// Now let's go hunt for the most recent one if there are any entries for that feed
		if(cur_feedResults.size() > 0)
		{
			cur_feedResults = cur_feedResults.sort(new BasicDBObject("timestamp", -1)); // Sort them by date; newest first
			
			// Get the next one since it will be the newest
			dbo_entry = cur_feedResults.next();
		} 
		else 
		{
			// Set it to null so when the result is examined they know we didn't
			// find anything. Currently has name = str_feedName;
			dbo_entry = null;
		}
		
		return dbo_entry;
	}
	
	/**Removes all entries before the specified date in the entry array
	 * @author connorwm
	 * @param dt_mostRecent
	 * @param dtf
	 * @param dbo_entries
	 * @return Array of DBObjects with dates that occur after dt_mostRecent
	 */
	private ArrayList<DBObject> removeEntriesBefore(Long l_mostRecent, ArrayList<DBObject> dbo_entries)
	{
		ArrayList<DBObject> dbo_toBeRemoved = new ArrayList<DBObject>();
		
		for(int i_entryIndex = 0; i_entryIndex < dbo_entries.size(); i_entryIndex++)
		{
			DBObject dbo_current = dbo_entries.get(i_entryIndex);
			
			if(Long.parseLong(String.valueOf(dbo_current.get("timestamp"))) <= l_mostRecent)
			{
				dbo_toBeRemoved.add(dbo_current);
			}
		}
		
		lh_slave.write("Removing "+dbo_toBeRemoved.size()+"/"+dbo_entries.size()+" of "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
		dbo_entries.removeAll(dbo_toBeRemoved);
		lh_slave.write("Entries to be posted of "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+": "+dbo_entries.size());
		
		return dbo_entries;
	}
	
	/**Returns the date of an entry regardless of being from an RSS or Atom feed
	 * @author connorwm
	 * @param dbo_entry - MongoDB object of Entry
	 * @param dtf - DateTimeFormatter for the feed's date format
	 * @return DateTime object of the entry's post date
	 */
	private DateTime getEntryDate(DBObject dbo_entry, DateTimeFormatter dtf)
	{
		DateTime dt_entryDate = null;
		
		// Since some are RSS and some are Atom, get the right date
		if(dbo_entry.get("timestamp") != null)
		{
			dt_entryDate = new DateTime(1970, 1, 1, 0, 0, 0, 0);
			dt_entryDate = dt_entryDate.plusSeconds(Integer.parseInt(String.valueOf(dbo_entry.get("timestamp"))));
		}
		else
		{
			if(dbo_entry.get("updated") != null) 
			{
				dt_entryDate = dtf.parseDateTime(String.valueOf(dbo_entry.get("updated")));
			}
			else 
			{
				dt_entryDate = dtf.parseDateTime(String.valueOf(dbo_entry.get("pubDate")));
			}
			
		}
		
		return dt_entryDate;
	}
	
	/**Returns a DBObject array parsed from the Entry array.
	* @author connorwm
	* @param col_entries - The entries collection for the feed
	* @param collectionEmpty - Boolean value for whether or not the feed's collection has any entries
	* @param entries - Array of Entry downloaded from the feed's Atom or RSS document
	*/
	private ArrayList<DBObject> buildEntryList(DBCollection col_entries, boolean collectionEmpty, ArrayList<Entry> entries)
	{
		ArrayList<DBObject> dbo_entries = new ArrayList<DBObject>();
		DBObject dbo_entry = new BasicDBObject();
		Entry e_entry = null;
		
		for(int entryIndex = 0; entryIndex < entries.size(); entryIndex++)
		{
			dbo_entry = new BasicDBObject();
			e_entry = entries.get(entryIndex);

			// Add Properties
			// Especially the feed that it came from
			dbo_entry.put("feed", (String)f_feed.get(Feed.Properties.name));

			for(int entryPropertyIndex = 0; entryPropertyIndex < Entry.Properties.values().length; entryPropertyIndex++)
			{
				if(e_entry.get(Entry.Properties.values()[entryPropertyIndex]) != null)
				{
					dbo_entry.put(Entry.Properties.values()[entryPropertyIndex].toString(), e_entry.get(Entry.Properties.values()[entryPropertyIndex]));
				}
			}
			// ---------------------------- Handle links -------------------------------------
			if(e_entry.getLinks().size() > 0)
			{
				BasicDBList dbl_links = new BasicDBList();

				for(int linkIndex = 0; linkIndex < e_entry.getLinks().size(); linkIndex++)
				{
					DBObject dbo_link = new BasicDBObject();
					Link l_link = e_entry.getLinks().get(linkIndex);

					for(int linkPropertyIndex = 0; linkPropertyIndex < Link.Properties.values().length; linkPropertyIndex++)
					{
						if(l_link.get(Link.Properties.values()[linkPropertyIndex]) != null)
						{
							dbo_link.put(Link.Properties.values()[linkPropertyIndex].toString(), l_link.get(Link.Properties.values()[linkPropertyIndex]));
						}
					}

					dbl_links.add(dbo_link);
				}

				dbo_entry.put("link", dbl_links);
			}
			// ---------------------------- Handle authors -------------------------------------
			if(e_entry.getAuthors().size() > 0)
			{
				BasicDBList dbl_authors = new BasicDBList();

				for(int authorIndex = 0; authorIndex < e_entry.getAuthors().size(); authorIndex++)
				{
					DBObject dbo_author = new BasicDBObject();
					Author a_author = e_entry.getAuthors().get(authorIndex);

					for(int authorPropertyIndex = 0; authorPropertyIndex < Author.Properties.values().length; authorPropertyIndex++)
					{
						if(a_author.get(Author.Properties.values()[authorPropertyIndex]) != null)
						{
							dbo_author.put(Author.Properties.values()[authorPropertyIndex].toString(), a_author.get(Author.Properties.values()[authorPropertyIndex]));
						}
					}

					dbl_authors.add(dbo_author);
				}

				dbo_entry.put("author", dbl_authors);
			}

			dbo_entries.add(dbo_entry);	
		}
		
		return dbo_entries;
	}
	
	/**Gets the corresponding feed document in database for the feed
	 * @author connorwm
	 * @param f_feed
	 * @param col_feeds
	 * @return
	 */
	private DBObject getFeedDocument(Feed f_feed, DBCollection col_feeds)
	{
		DBObject dbo_feed = null;
		DBObject dbo_searchQuery = new BasicDBObject();
		String str_url = f_feed.get(Feed.Properties.url);
		
		// Now find the collection and get the feed's object from the feeds database
		dbo_searchQuery.put("name", f_feed.get(Feed.Properties.name));
		dbo_feed = col_feeds.findOne(dbo_searchQuery);
		
		lh_slave.write("Feed Search Query: "+dbo_searchQuery.toString());
		
		if(dbo_feed == null) dbo_feed = new BasicDBObject(); // Does not exist, create it
		
		lh_slave.write("Feed Result:       "+dbo_feed.toString());
		
		return dbo_feed;		
	}
	
	/**Gets the statistics document for the corresponding feed.
	 * @author connorwm
	 * @param f_feed - The feed document
	 * @param col_feedStatistics - MongoDB statistics collection
	 * @return dbo_feedStatistics or null if the document does not exist
	 */
	private DBObject getStatisticsDocument(Feed f_feed, DBCollection col_feedStatistics)
	{
		DBObject dbo_searchTerm = new BasicDBObject();

		dbo_searchTerm.put("name", f_feed.get(Feed.Properties.name));
		dbo_searchTerm.put("collection", f_feed.get(Feed.Properties.collection));

		DBObject dbo_feedStatistics = col_feedStatistics.findOne(dbo_searchTerm);
		
		if(dbo_feedStatistics != null)
		{
			lh_slave.write("Feed Statistics: "+dbo_feedStatistics.toString());
		} else {
			lh_slave.write("Feed Statistics Null");
		}
		
		return dbo_feedStatistics;
	}
	
	/**Updates the statistics document for a feed
	 * @author connorwm
	 * @param f_feed - The feed for the statistics
	 * @param dbo_feedStatistics - The statistics document to be posted
	 * @param col_feedStatistics - The statistics collection
	 */
	private void updateStatistics(Feed f_feed, DBObject dbo_feedStatistics, DBCollection col_feedStatistics)
	{
		WriteResult wr_result = null;
		
		lh_slave.write("Updating statistics for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
		lh_slave.write("New statics document for "+f_feed.get(Feed.Properties.name)+": "+dbo_feedStatistics.toString());
		
		// But we need to change the timestamp
		dbo_feedStatistics.put("timestamp", f_feed.get(Feed.Properties.timestamp));				
		lh_slave.write("Added timestamp of "+f_feed.get(Feed.Properties.timestamp)+" to statistics of "+f_feed.get(Feed.Properties.name));

		// And now submit
		try {
			wr_result = col_feedStatistics.update(new BasicDBObject("_id", dbo_feedStatistics.get("_id")), dbo_feedStatistics);		
		} 
		catch (MongoException excep_m)
		{
			lh_slave.write("Failed to update statistics for "+f_feed.get(Feed.Properties.name)+" due to MongoException: "+excep_m.toString());
		}
		finally
		{
			if(wr_result.getN() == 1) lh_slave.write("Successfully updated statistics for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
			else 
			{
				lh_slave.write("Failed to update statistics for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"\r\nN: "+wr_result.getN()+"\r\n"+wr_result.getError());
			}
		}
	}
	
	/**Post entries to the feed's collection
	 * @author connorwm
	 * @param arr_entries - ArrayList of type DBObject of the entries to be posted
	 * @param col_entries - The feed's entry collection
	 */
	private void postEntries(ArrayList<DBObject> arr_entries, DBCollection col_entries)
	{
		lh_slave.write("Posting "+arr_entries.size()+" entries for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
		WriteResult insertResult = col_entries.insert(arr_entries);
		
		if(insertResult.getError() == null)
		{
			lh_slave.write("Successfully posted entries for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
			
			// Only update the statistics when we actually successfully post
			updateStatistics(f_feed, dbo_feedStatistics, col_feedStatistics);
		} else {
			lh_slave.write("Could not post entries for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"; Last error:\r\n"+insertResult.getError());
		}
	}
	
	/**Run each entry through an instance of Processor to get the keywords
	 *  @author connorwm
	 * 	@param arr_entries - List of entries from a parsed feed
	 */
	private void processEntries(ArrayList<DBObject> arr_entries)
	{		
		lh_slave.write("Processing entries for "+f_feed.get(Feed.Properties.collection)+":"+f_feed.get(Feed.Properties.name)+"...");
		
		for(DBObject dbo_entry : arr_entries)
		{
			p_processor = new Processor((String)dbo_entry.get("description"), "UTF-8", s_settings.getProcessorIgnoreWords(), null, f_feed.get(Feed.Properties.collection), dx_executor, l_main); 
			p_processor.run();
		}
		
		lh_slave.write("Processing complete");
	}
	
	public void setFuture(Future<?> ftr_future)
	{
		this.ftr_future = ftr_future;
	}
	
	public Future<?> getFuture()
	{
		return ftr_future;
	}
}