package net.staplr.processing;

import java.util.ArrayList;
import java.util.Collections;

import org.bson.Document;
import org.joda.time.DateTime;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.result.UpdateResult;

import net.staplr.common.feed.Feed;
import net.staplr.logging.LogHandle;
import net.staplr.logging.Entry.Type;
import net.staplr.master.DatabaseExecutor;

public class Result
{
	public ArrayList<Keyword> arr_keyword;
	public ArrayList<Term> arr_term;
	private DatabaseExecutor dx_executor;
	private LogHandle lh_processor;
	
	public Result(DatabaseExecutor dx_executor, LogHandle lh_processor)
	{
		this.dx_executor = dx_executor;
		this.lh_processor = lh_processor;
		
		arr_keyword = new ArrayList<Keyword>();
		arr_term = new ArrayList<Term>();
	}
	
	public void post(String str_entryFeed, String str_entryID)
	{
		lh_processor.write("Sorting keywords");
		Collections.sort(arr_keyword, new KeywordComparator());
		lh_processor.write("Sorted keywords");
		
		lh_processor.write("Picking top keywords..."+(arr_keyword.size() - 1));
		
		int i_currentKeywordOccurences = arr_keyword.get(arr_keyword.size()-1).getOccurences();
		ArrayList<Keyword> arr_topKeyword = new ArrayList<Keyword>();
		
		for(int i_keyword = (arr_keyword.size() - 1); i_keyword > 0; i_keyword--)
		{
			//System.out.println(arr_keyword.get(i_keyword).toString()+"["+arr_keyword.get(i_keyword).getOccurences()+":"+i_currentKeywordOccurences+"]");
			if(arr_keyword.get(i_keyword).getOccurences() < i_currentKeywordOccurences)
			{
				i_currentKeywordOccurences = arr_keyword.get(i_keyword).getOccurences();
				
				//System.out.println("\tDropping occurence count");
				if(arr_topKeyword.size() >= 10)
				{
					//System.out.println("\t\tHave enough; breaking");
					// We have at least 5 keywords and the rest have less occurrences so
					// we do not need them
					break;
				}
				else
				{
					//System.out.println("\t\tAdding because we don't even have 10");
					arr_topKeyword.add(arr_keyword.get(i_keyword));
				}
			}
			else if(arr_keyword.get(i_keyword).getOccurences() == i_currentKeywordOccurences && arr_topKeyword.size() <= 10)
			{
				//System.out.println("\tAdding because its the same count and we don't even have 10");
				arr_topKeyword.add(arr_keyword.get(i_keyword));
				i_currentKeywordOccurences = arr_keyword.get(i_keyword).getOccurences();
			}
		}
		
		arr_keyword = arr_topKeyword;
		
		lh_processor.write("Picked "+arr_keyword.size()+" keywords");
		
		DateTime dt_now = new DateTime(1970, 1, 1, 0, 0, 0, 0);
		dt_now = dt_now.plusSeconds((int)((new DateTime()).getMillis() / 1000));
		
		for(Keyword k_word : arr_keyword)
		{
			MongoCollection<Document> col_word = getCollection(k_word.toString());
			
			if(col_word != null)
			{
				Document doc_association = new Document();
				
				doc_association.put("entryID", str_entryID);
				doc_association.put("entryFeed", str_entryFeed);
				doc_association.put("timestamp", (dt_now.getMillis() / 1000)); // TEMP
				doc_association.put("occurences",  k_word.getOccurences());
				
				// TEMP: Do not post as of yet
//				WriteResult wr_result = col_word.insert(doc_association);
//				
//				if(wr_result.getN() == 1) lh_processor.write("Successfully associated '"+k_word.toString()+"' with "+str_entryFeed+":"+str_entryID);
//				else lh_proccessor.write("Failed to associate '"+k_word.toString()+"' with "+str_entryFeed+":"+str_entryID);
				
				//TEMP: For debug only:
				lh_processor.write("Posted: "+k_word.toString());
			}
			else
			{
				lh_processor.write(Type.Error, "Failed to get collection for '"+k_word.toString()+"'");
			}
		}
		
		// TEMP: Do not post for now until bugs have been worked out
		//postTrendData(arr_keyword);
	}
	
	private MongoCollection<Document> getCollection(String str_name)
	{
		MongoCollection<Document> collection = null;
		
		if(!collectionExists(dx_executor.db_associations.listCollectionNames(), str_name))
		{
			dx_executor.db_associations.createCollection(str_name);
		}
		
		collection = dx_executor.db_associations.getCollection(str_name);
		
		return collection;
	}
	
	private void postTrendData(ArrayList<Keyword> arr_keywords)
	{
		MongoCollection<Document> col_trends = getCollection("_trends");
		DateTime dt_now = new DateTime();
		String str_currentTrendDocument = dt_now.toString("M-d-yyyy");
		Document doc_query = new Document();
		Document doc_currentTrendDocument = null;
		
		doc_query.put("_date", str_currentTrendDocument);
		doc_currentTrendDocument = col_trends.find(doc_query).first();
		MongoCursor<Document> cur_trendResult = col_trends.find(doc_query).iterator();
		int i_trendDocumentCount = 0;
		
		// Due to there being no way to get a count from the iterator itself, count using a cursor
		// until we run out of documents
		while(cur_trendResult.hasNext()) {
			i_trendDocumentCount++;
			cur_trendResult.next();			
		}
		
		if(doc_currentTrendDocument != null)
		{
			lh_processor.write("Found trend document");
		}
		else
		{
			lh_processor.write("Trend document not found; will create one");
			
			doc_currentTrendDocument = new Document();
			doc_currentTrendDocument.put("_date", str_currentTrendDocument);
		}
		
		// Log notification if necessary for multiple trend documents
		if(i_trendDocumentCount > 1)
		{
			lh_processor.write(Type.Warning, "Multiple trend documents found for "+str_currentTrendDocument+"; administrative action necessary");
		}
		
		if(doc_currentTrendDocument != null)
		{
			for(Keyword k_keyword : arr_keywords)
			{
				if(doc_currentTrendDocument.containsKey(k_keyword.toString()))
				{
					int i_occurences = -1;
					
					try
					{
						i_occurences = (Integer)doc_currentTrendDocument.get(k_keyword.toString());
					}
					catch (Exception e) 
					{
						lh_processor.write(Type.Error, "Could not parse occurence count in "+str_currentTrendDocument+" for "+k_keyword.toString());
						i_occurences = 0;
					}
					
					i_occurences += 1;
					
					doc_currentTrendDocument.put(k_keyword.toString(), i_occurences);					
				}
				else
				{
					doc_currentTrendDocument.put(k_keyword.toString(), 1);		
				}
			}
			
			if(i_trendDocumentCount >= 1)
			{
				UpdateResult ur_updateOccurences = col_trends.updateOne(doc_query, doc_currentTrendDocument);
				
				if(ur_updateOccurences.getModifiedCount() == 1)
				{
					lh_processor.write("Successfully updated "+str_currentTrendDocument+" trend document");
				}
				else
				{
					lh_processor.write(Type.Error, "Failed to update trends entry for "+str_currentTrendDocument);
				}
			}
			else
			{
				try {
					col_trends.insertOne(doc_currentTrendDocument);
				} 
				catch (MongoWriteException excep_write)
				{
					lh_processor.write(Type.Error, "Failed to insert trends document for "+str_currentTrendDocument+" due to MongoWriteException: "+excep_write.getError().toString());
				}
				catch (MongoWriteConcernException excep_writeConcern)
				{
					lh_processor.write(Type.Error, "Failed to insert trends document for "+str_currentTrendDocument+" due to MongoWriteConcernException: "+excep_writeConcern.getMessage().toString());
				}
				catch (MongoException excep_m)
				{
					lh_processor.write(Type.Error, "Failed to insert trends document for "+str_currentTrendDocument+" due to MongoException: "+excep_m.toString());
				}
				finally
				{
					lh_processor.write("Successfully inserted "+str_currentTrendDocument+" trend document");
				}
			}
			
		}
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
}