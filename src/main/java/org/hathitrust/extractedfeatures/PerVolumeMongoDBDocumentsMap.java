package org.hathitrust.extractedfeatures;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.DoubleAccumulator;
import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


class PerVolumeMongoDBDocumentsMap implements Function<String, Integer>
{
	private static final long serialVersionUID = 1L;
	
	protected String _input_dir;
	protected int    _verbosity;
	
	protected DoubleAccumulator _progress_accum;
	protected double            _progress_step;
	
	boolean _strict_file_io;
	
	public PerVolumeMongoDBDocumentsMap(String input_dir, int verbosity, 
					 		  DoubleAccumulator progress_accum, double progress_step,
					 		  boolean strict_file_io)
	{
		_input_dir  = input_dir;
		_verbosity  = verbosity;
		
		_progress_accum = progress_accum;
		_progress_step  = progress_step;
		
		_strict_file_io = strict_file_io;
	}
	
	protected void fixup_section(Document ef_count)
	{
		
		Set<String> key_set = ef_count.keySet();
		String[] key_array = key_set.toArray(new String[key_set.size()]);
		
		for (int i=0; i<key_array.length; i++) {
			
			String key = key_array[i];
			//String key = key_iterator.next();
			if (key.contains(".")) {
				String new_key = key.replaceAll("\\.", "<PERIOD>");
				//System.out.println("**** old key:" + key + "=> new key:" + new_key);
				ef_count.put(new_key, ef_count.get(key));
				ef_count.remove(key);
				key = new_key;
			}

			if (key.contains("$")) {
				String new_key = key.replaceAll("\\$", "<DOLLAR>");
				ef_count.put(new_key, ef_count.get(key));
				ef_count.remove(key);
			}

		}		
	}
	
	protected  void fixup_page(String volume_id, String page_id, Document ef_page) 
	{
		if (ef_page != null) {
			String[] zone_keys = { "header", "body", "footer" };
			
			for (String zone_key: zone_keys) {
				Document ef_zone = (Document)ef_page.get(zone_key);
				if (ef_zone != null) {
					String[] count_keys = { "beginCharCounts", "endCharCount", "tokenPosCount" };
					
					for (String count_key: count_keys) {
						Document ef_sub_section = (Document)ef_zone.get(count_key);
						if (ef_sub_section != null) {
							fixup_section(ef_sub_section);
							
							if (count_key.equals("tokenPosCount")) {
								Set<String> key_set = ef_sub_section.keySet();
								for (String key : key_set) {
									Document token_section = (Document)ef_sub_section.get(key);
									fixup_section(token_section);
								}
							}
						
							
						}
					}
				}
			}
		}
		else {
			System.err.println("Warning: null page for '" + page_id + "'");
		}

	}
	protected void fixup_volume(String json_file_in, Document extracted_feature_record)
	{
		String full_json_file_in = _input_dir + "/" + json_file_in;
		
		if (extracted_feature_record != null) {
			String volume_id = extracted_feature_record.getString("id");
			extracted_feature_record.put("_id",volume_id);
			extracted_feature_record.remove("id");
			
			Document ef_features = (Document)extracted_feature_record.get("features");

			int ef_page_count = ef_features.getInteger("pageCount");

			if (_verbosity >= 1) {
				System.out.println("Processing: " + json_file_in);
				System.out.println("  pageCount = " + ef_page_count);
			}

			List<Document> ef_pages = (List<Document>)ef_features.get("pages");
			int ef_num_pages = ef_pages.size();
			if (ef_num_pages != ef_page_count) {
				System.err.println("Warning: number of page elements in JSON (" + ef_num_pages + ")"
						+" does not match 'pageCount' metadata (" + ef_page_count + ")"); 
			}
	
			if (_verbosity >= 2) {
				System.out.print("  Pages: ");
			}

			for (int i = 0; i < ef_page_count; i++) {
				String formatted_i = String.format("page-%06d", i);
				String page_id = volume_id + "." + formatted_i;

				if (_verbosity >= 2) {
					if (i>0) {
						System.out.print(", ");
					}
					System.out.print(page_id);
				}

				if (i==(ef_page_count-1)) {
					if (_verbosity >= 2) {
						System.out.println();
					}
				}

				Document ef_page = (Document)ef_pages.get(i);

				if (ef_page != null) {
					
					fixup_page(volume_id, page_id, ef_page);					
				}
				else {
					System.err.println("Skipping: " + page_id);
				}
			}
		}
		else {
			// File did not exist, or could not be parsed
			String mess = "Failed to read in bzipped JSON file '" + full_json_file_in + "'";
		
			System.err.println("Warning: " + mess);
			System.out.println("Warning: " + mess);
		
		}
	}
	
	public Integer call(String json_file_in) throws IOException
	{ 
		try {
			MongoClientURI mongo_url = new MongoClientURI("mongodb://gc3:27017,gc4:27017,gc5:27017");
			MongoClient mongoClient = new MongoClient(mongo_url);
			
			MongoDatabase database = mongoClient.getDatabase("htrc_ef");
			MongoCollection<Document> collection = database.getCollection("volumes");
			
			String full_json_file_in = _input_dir + "/" + json_file_in;
			System.out.println("Processing: " + full_json_file_in);
			String extracted_feature_json_doc = ClusterFileIO.readTextFile(full_json_file_in);
			
			Document doc = Document.parse(extracted_feature_json_doc);
			
			fixup_volume(json_file_in,doc);
			
			collection.findOneAndReplace(new Document("_id",doc.getString("_id")), doc);
			
			//collection.insertOne(doc);		
			
			/*
			//Mongo mongo = new Mongo("localhost", 27017);
			MongoClient mongo = new MongoClient( "localhost" , 27017 );
			
			DB db = mongo.getDB("yourdb");
			DBCollection coll = db.getCollection("dummyColl");

			// convert JSON to DBObject directly
			DBObject dbObject = (DBObject) JSON
					.parse("{'name':'mkyong', 'age':30}");
			coll.insert(dbObject);

			
			DBCursor cursorDoc = coll.find();
			while (cursorDoc.hasNext()) {
				System.out.println(cursorDoc.next());
			}

			System.out.println("Done");
*/
			mongoClient.close();
			
		} catch (MongoException e) {
			e.printStackTrace();
		}

		return 1;
	}
	public Integer callPageCount(String json_file_in) throws IOException
	{ 
	    Integer page_count = 0;
	    
		String full_json_file_in = _input_dir + "/" + json_file_in;
		JSONObject extracted_feature_record = JSONClusterFileIO.readJSONFile(full_json_file_in);
		
		if (extracted_feature_record != null) {
			String volume_id = extracted_feature_record.getString("id");

			JSONObject ef_features = extracted_feature_record.getJSONObject("features");

			if (_verbosity >= 1) {
				System.out.println("Processing: " + json_file_in);
			}

			if (ef_features != null) {
				String page_count_str = ef_features.getString("pageCount");
				if (!page_count_str.equals("")) {
					page_count = Integer.parseInt(page_count_str);
						}
				else {
					System.err.println("No 'pageCount' in 'features' in volume id '" + volume_id + "' => defaulting to 0");
				}
			}
			else {
				System.err.println("No 'features' section in JSON file => Skipping id: " + volume_id);
			}
			
		}
		else {
			// File did not exist, or could not be parsed
			String mess = "Failed to read in bzipped JSON file '" + full_json_file_in + "'";
			if (_strict_file_io) {
				throw new IOException(mess);
			}
			else {
				System.err.println("Warning: " + mess);
				System.out.println("Warning: " + mess);
			}
		}

		_progress_accum.add(_progress_step);
		
		return page_count;
	}
	
	
}

