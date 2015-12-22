package net.staplr.slave;

import java.util.ArrayList;
import java.util.concurrent.Future;

import org.bson.Document;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import net.staplr.common.Settings;
import net.staplr.common.feed.Author;
import net.staplr.common.feed.Entry;
import net.staplr.common.feed.Feed;
import net.staplr.common.feed.FeedDocument;
import net.staplr.common.feed.Link;
import net.staplr.logging.Log;
import net.staplr.logging.Entry.Type;
import net.staplr.logging.LogHandle;
import net.staplr.master.DatabaseExecutor;
import net.staplr.processing.Processor;

import com.mongodb.BasicDBList;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;

/**Essential runnable object for updating and parsing a feed
 * @author connorwm
 */
public class Slave implements Runnable
{
	private FeedParser feedParser;
	
	private MongoDatabase db_feed;
	private MongoDatabase db_entries;
	private MongoDatabase db_statistics;
	
	private Settings s_settings;
	private Feed f_feed;
	private LogHandle lh_slave;
	private Document doc_feedStatistics;
	private MongoCollection<Document> col_feedStatistics;
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
		doc_feedStatistics = getStatisticsDocument(f_feed, col_feedStatistics);	
		
		// Get the collections because we will need their documents
		MongoCollection<Document> col_entries = db_entries.getCollection(f_feed.get(Feed.Properties.collection));
		MongoCollection<Document> col_feeds = db_feed.getCollection(f_feed.get(Feed.Properties.collection));
		Document doc_feed = getFeedDocument(f_feed, col_feeds);
		
		// Check entries collection
		
		
		if(!collectionExists(db_entries.listCollectionNames(), f_feed.get(Feed.Properties.collection)))
		{
			lh_slave.write("Entries collection "+f_feed.get(Feed.Properties.collection)+" does not exist.");
		} else {		
			lh_slave.write("Entries from "+f_feed.get(Feed.Properties.collection)+" contains "+col_entries.count());
		}
		
		// Check feeds collection
		if(!collectionExists(db_feed.listCollectionNames(), f_feed.get(Feed.Properties.collection)))
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
			Downloader feedDownloader = new Downloader(f_feed.get(Feed.Properties.url), s_settings, str_feedDateFormat, lh_slave);

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
				feedParser = new FeedParser(fd_feedDocument, f_feed, doc_feed, lh_slave);
				feedParser.parse();
				lh_slave.write("Parsed "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+":\t"+doc_feed.toString());
				
				lh_slave.write("Posting updated feed document of "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
				UpdateResult ur_feeds = col_feeds.updateOne(new Document("_id", doc_feed.get("_id")), doc_feed);
				
				if(ur_feeds.getModifiedCount() == 1) 
				{
					lh_slave.write("Successfully updated "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
				}
				else 
				{
					lh_slave.write(Type.Error, "Failed to update feed document of "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"\r\n: "+
							"Matched: "+ur_feeds.getMatchedCount()+"; Modified: "+ur_feeds.getModifiedCount()+"; Message: "+ur_feeds.toString());
				}
				
				//--------------------------------------------------------------------------
				// Parse entries
				//--------------------------------------------------------------------------
				
				lh_slave.write("Working on entries for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+":");

				Document doc_entry = null;
				ArrayList<Document> arr_entries = new ArrayList<Document>();
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
				
				//  If doc_entries has entries, then we need to figure out which ones need to be posted
				if(arr_entries.size() > 0)
				{
					lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" has entries to be posted. "+arr_entries.size()+" to be parsed.");
					
					if(collectionEmpty)
					{
						lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" has no entries; posting all");
					}
					else
					{
						if(doc_feedStatistics != null)
						{
							lh_slave.write("Have statistics document for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
							
							if(doc_feedStatistics.get("timestamp") != null)
							{
								lh_slave.write(f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+" has most recent date; removing entries before...");
								arr_entries = removeEntriesBefore(Long.parseLong(String.valueOf(doc_feedStatistics.get("timestamp"))), arr_entries);
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
			lh_slave.write(Type.Error, "\tURL is null");
		}
		
		System.gc();
	}
	
	/**Removes all entries in the entries array that are older than the most recent entry in the specified feed
	 * @author connorwm
	 * @param doc_entries - Array of entries that contains old entries
	 * @param f_feed - The corresponding feed
	 * @param col_entries - The entry collection corresponding to the feed
	 * @return Document array with old entries removed
	 */
	private ArrayList<Document> removeOldEntries(ArrayList<Document> doc_entries, Feed f_feed, MongoCollection<Document> col_entries)
	{
		Document doc_mostRecentEntry = findMostRecentEntry(f_feed.get(Feed.Properties.name), col_entries);
		
		if(doc_mostRecentEntry != null)
		{
			lh_slave.write("Found a most recent entry for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"; removing others before...");
			doc_entries = removeEntriesBefore(Long.valueOf(String.valueOf(doc_mostRecentEntry.get("timestamp"))), doc_entries);
			lh_slave.write("Old entries removed from "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"; ready to post");
		}
		else
		{
			lh_slave.write("Could not find a most recent entry for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"; all will be posted");
		}
		
		return doc_entries;
	}
	
	/**Finds the most recent entry in the the feed specified by str_feedName in the feed's collection
	 * @author connorwm
	 * @param str_feedName
	 * @param col_entryCollection
	 * @return Document with the most recent entry
	 */
	private Document findMostRecentEntry(String str_feedName, MongoCollection<Document> col_entryCollection)
	{
		Document doc_entry = new Document();
		MongoCursor<Document> cur_feedResults = null;
		
		// Find all entries in the collection for that collection's individual feed
		// Example: Gizmodo
		// We want to find the entries from the 'top' feed of Gizmodo. So set name = 'top';
		doc_entry.put("feed", str_feedName);
		
		MongoIterable<Document> it_feedResults = col_entryCollection.find().sort(new Document("timestamp", -1)); // Sort them by date; newest first
		
		// Now let's go get the most recent one if there are any entries for that feed
		if(it_feedResults.first() != null)
		{			
			doc_entry = (Document) it_feedResults.first();
		} 
		else 
		{
			// Set it to null so when the result is examined they know we didn't
			// find anything. Currently has name = str_feedName;
			doc_entry = null;
		}
		
		return doc_entry;
	}
	
	/**Removes all entries before the specified date in the entry array
	 * @author connorwm
	 * @param dt_mostRecent
	 * @param dtf
	 * @param doc_entries
	 * @return Array of DBObjects with dates that occur after dt_mostRecent
	 */
	private ArrayList<Document> removeEntriesBefore(Long l_mostRecent, ArrayList<Document> doc_entries)
	{
		ArrayList<Document> doc_toBeRemoved = new ArrayList<Document>();
		
		for(int i_entryIndex = 0; i_entryIndex < doc_entries.size(); i_entryIndex++)
		{
			Document doc_current = doc_entries.get(i_entryIndex);
			
			if(Long.parseLong(String.valueOf(doc_current.get("timestamp"))) <= l_mostRecent)
			{
				doc_toBeRemoved.add(doc_current);
			}
		}
		
		lh_slave.write("Removing "+doc_toBeRemoved.size()+"/"+doc_entries.size()+" of "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
		doc_entries.removeAll(doc_toBeRemoved);
		lh_slave.write("Entries to be posted of "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+": "+doc_entries.size());
		
		return doc_entries;
	}
	
	/**Returns the date of an entry regardless of being from an RSS or Atom feed
	 * @author connorwm
	 * @param doc_entry - MongoDB object of Entry
	 * @param dtf - DateTimeFormatter for the feed's date format
	 * @return DateTime object of the entry's post date
	 */
	private DateTime getEntryDate(Document doc_entry, DateTimeFormatter dtf)
	{
		DateTime dt_entryDate = null;
		
		// Since some are RSS and some are Atom, get the right date
		if(doc_entry.get("timestamp") != null)
		{
			dt_entryDate = new DateTime(1970, 1, 1, 0, 0, 0, 0);
			dt_entryDate = dt_entryDate.plusSeconds(Integer.parseInt(String.valueOf(doc_entry.get("timestamp"))));
		}
		else
		{
			if(doc_entry.get("updated") != null) 
			{
				dt_entryDate = dtf.parseDateTime(String.valueOf(doc_entry.get("updated")));
			}
			else 
			{
				dt_entryDate = dtf.parseDateTime(String.valueOf(doc_entry.get("pubDate")));
			}
			
		}
		
		return dt_entryDate;
	}
	
	/**Returns a Document array parsed from the Entry array.
	* @author connorwm
	* @param col_entries - The entries collection for the feed
	* @param collectionEmpty - Boolean value for whether or not the feed's collection has any entries
	* @param entries - Array of Entry downloaded from the feed's Atom or RSS document
	*/
	private ArrayList<Document> buildEntryList(MongoCollection<Document> col_entries, boolean collectionEmpty, ArrayList<Entry> entries)
	{
		ArrayList<Document> doc_entries = new ArrayList<Document>();
		Document doc_entry = new Document();
		Entry e_entry = null;
		
		for(int entryIndex = 0; entryIndex < entries.size(); entryIndex++)
		{
			doc_entry = new Document();
			e_entry = entries.get(entryIndex);

			// Add Properties
			// Especially the feed that it came from
			doc_entry.put("feed", (String)f_feed.get(Feed.Properties.name));

			for(int entryPropertyIndex = 0; entryPropertyIndex < Entry.Properties.values().length; entryPropertyIndex++)
			{
				if(e_entry.get(Entry.Properties.values()[entryPropertyIndex]) != null)
				{
					doc_entry.put(Entry.Properties.values()[entryPropertyIndex].toString(), e_entry.get(Entry.Properties.values()[entryPropertyIndex]));
				}
			}
			// ---------------------------- Handle links -------------------------------------
			if(e_entry.getLinks().size() > 0)
			{
				BasicDBList dbl_links = new BasicDBList();

				for(int linkIndex = 0; linkIndex < e_entry.getLinks().size(); linkIndex++)
				{
					Document doc_link = new Document();
					Link l_link = e_entry.getLinks().get(linkIndex);

					for(int linkPropertyIndex = 0; linkPropertyIndex < Link.Properties.values().length; linkPropertyIndex++)
					{
						if(l_link.get(Link.Properties.values()[linkPropertyIndex]) != null)
						{
							doc_link.put(Link.Properties.values()[linkPropertyIndex].toString(), l_link.get(Link.Properties.values()[linkPropertyIndex]));
						}
					}

					dbl_links.add(doc_link);
				}

				doc_entry.put("link", dbl_links);
			}
			// ---------------------------- Handle authors -------------------------------------
			if(e_entry.getAuthors().size() > 0)
			{
				BasicDBList dbl_authors = new BasicDBList();

				for(int authorIndex = 0; authorIndex < e_entry.getAuthors().size(); authorIndex++)
				{
					Document doc_author = new Document();
					Author a_author = e_entry.getAuthors().get(authorIndex);

					for(int authorPropertyIndex = 0; authorPropertyIndex < Author.Properties.values().length; authorPropertyIndex++)
					{
						if(a_author.get(Author.Properties.values()[authorPropertyIndex]) != null)
						{
							doc_author.put(Author.Properties.values()[authorPropertyIndex].toString(), a_author.get(Author.Properties.values()[authorPropertyIndex]));
						}
					}

					dbl_authors.add(doc_author);
				}

				doc_entry.put("author", dbl_authors);
			}

			doc_entries.add(doc_entry);	
		}
		
		return doc_entries;
	}
	
	/**Gets the corresponding feed document in database for the feed
	 * @author connorwm
	 * @param f_feed
	 * @param col_feeds
	 * @return
	 */
	private Document getFeedDocument(Feed f_feed, MongoCollection<Document> col_feeds)
	{
		Document doc_feed = null;
		Document doc_searchQuery = new Document();
		String str_url = f_feed.get(Feed.Properties.url);
		
		// Now find the collection and get the feed's object from the feeds database
		doc_searchQuery.put("name", f_feed.get(Feed.Properties.name));
		doc_feed = (Document) col_feeds.find(doc_searchQuery);
		
		lh_slave.write("Feed Search Query: "+doc_searchQuery.toString());
		
		if(doc_feed == null) doc_feed = new Document(); // Does not exist, create it
		
		lh_slave.write("Feed Result:       "+doc_feed.toString());
		
		return doc_feed;		
	}
	
	/**Gets the statistics document for the corresponding feed.
	 * @author connorwm
	 * @param f_feed - The feed document
	 * @param col_feedStatistics - MongoDB statistics collection
	 * @return doc_feedStatistics or null if the document does not exist
	 */
	private Document getStatisticsDocument(Feed f_feed, MongoCollection<Document> col_feedStatistics)
	{
		Document doc_searchTerm = new Document();

		doc_searchTerm.put("name", f_feed.get(Feed.Properties.name));
		doc_searchTerm.put("collection", f_feed.get(Feed.Properties.collection));

		Document doc_feedStatistics = (Document) col_feedStatistics.find(doc_searchTerm).limit(1);
		
		if(doc_feedStatistics != null)
		{
			lh_slave.write("Feed Statistics: "+doc_feedStatistics.toString());
		} else {
			lh_slave.write("Feed Statistics Null");
		}
		
		return doc_feedStatistics;
	}
	
	/**Updates the statistics document for a feed
	 * @author connorwm
	 * @param f_feed - The feed for the statistics
	 * @param doc_feedStatistics - The statistics document to be posted
	 * @param col_feedStatistics - The statistics collection
	 */
	private void updateStatistics(Feed f_feed, Document doc_feedStatistics, MongoCollection<Document> col_feedStatistics)
	{
		UpdateResult ur_feeds = null;
		
		lh_slave.write("Updating statistics for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
		lh_slave.write("New statics document for "+f_feed.get(Feed.Properties.name)+": "+doc_feedStatistics.toString());
		
		// But we need to change the timestamp
		doc_feedStatistics.put("timestamp", f_feed.get(Feed.Properties.timestamp));				
		lh_slave.write("Added timestamp of "+f_feed.get(Feed.Properties.timestamp)+" to statistics of "+f_feed.get(Feed.Properties.name));

		// And now submit
		try {
			ur_feeds = col_feedStatistics.updateOne(new Document("_id", doc_feedStatistics.get("_id")), doc_feedStatistics);		
		} 
		catch (MongoException excep_m)
		{
			lh_slave.write(Type.Error, "Failed to update statistics for "+f_feed.get(Feed.Properties.name)+" due to MongoException: "+excep_m.toString());
		}
		finally
		{
			if(ur_feeds.getModifiedCount() == 1) lh_slave.write("Successfully updated statistics for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
			else 
			{
				lh_slave.write(Type.Error, "Failed to update statistics for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+"\r\n: "+
								"Matched: "+ur_feeds.getMatchedCount()+"; Modified: "+ur_feeds.getModifiedCount()+"; Message: "+ur_feeds.toString());
			}
		}
	}
	
	/**Post entries to the feed's collection
	 * @author connorwm
	 * @param arr_entries - ArrayList of type Document of the entries to be posted
	 * @param col_entries - The feed's entry collection
	 */
	private void postEntries(ArrayList<Document> arr_entries, MongoCollection<Document> col_entries)
	{
		lh_slave.write("Posting "+arr_entries.size()+" entries for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
		
		try{
			col_entries.insertMany(arr_entries);
		}
		catch(MongoBulkWriteException excep_bulkWrite)
		{
			lh_slave.write(Type.Error, "Failed to post entries to "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+": \r\n"+excep_bulkWrite.getWriteErrors().toString());
		}
		catch(MongoException excep_other)
		{
			lh_slave.write(Type.Error, "Failed to post entries to "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection)+": \r\n"+excep_other.getMessage());
		}
		finally
		{
			lh_slave.write("Successfully posted entries for "+f_feed.get(Feed.Properties.name)+" from "+f_feed.get(Feed.Properties.collection));
			
			// Only update the statistics when we actually successfully post
			updateStatistics(f_feed, doc_feedStatistics, col_feedStatistics);
		}
	}
	
	/**Run each entry through an instance of Processor to get the keywords
	 *  @author connorwm
	 * 	@param arr_entries - List of entries from a parsed feed
	 */
	private void processEntries(ArrayList<Document> arr_entries)
	{		
		lh_slave.write("Processing entries for "+f_feed.get(Feed.Properties.collection)+":"+f_feed.get(Feed.Properties.name)+"...");
		
		for(Document doc_entry : arr_entries)
		{
			p_processor = new Processor((String)doc_entry.get("description"), "UTF-8", s_settings.getProcessorIgnoreWords(), null, f_feed.get(Feed.Properties.collection), dx_executor, l_main); 
			p_processor.run();
		}
		
		lh_slave.write("Processing complete");
	}
	
	/**Given a database's collection name iterator and name determine if the collection exists in the database
	 * @author connorwm
	 * @param it_collectionNames Database collection name iterator
	 * @param str_name Name of collection to find in database
	 * @return
	 */
	private boolean collectionExists(MongoIterable<String> it_collectionNames, String str_name)
	{
		boolean b_found = false;
		MongoCursor<String> cur_collectionNames = it_collectionNames.iterator();
		String str_currentCollectionName = cur_collectionNames.next();
		
		while(cur_collectionNames.hasNext())
		{
			if(str_currentCollectionName == str_name) {
				b_found = true;
				break;
			}
			
			str_currentCollectionName = cur_collectionNames.next();
		}
		
		return b_found;
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