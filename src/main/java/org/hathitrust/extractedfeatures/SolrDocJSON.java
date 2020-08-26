package org.hathitrust.extractedfeatures;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
//import java.util.Base64;
import org.apache.commons.codec.binary.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.nio.charset.StandardCharsets;
    
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import scala.Tuple2;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.core.LowerCaseFilter;

public abstract class SolrDocJSON implements Serializable {

	public final static int NUM_ALT_RETRIES = 10;
	
	protected String [] metadata_single_string = null;
	protected String [] metadata_multiple = null;

	
	public SolrDocJSON()
	{
	}
	
	abstract protected JSONObject generateMetadataSolrDocJSON(String id, JSONObject ef_metadata, boolean is_page_level);
	
	protected JSONObject generateToplevelMetadataSolrDocJSON(String volume_id, JSONObject ef_metadata)
	{
		JSONObject solr_update_json = null;
		
		if ((ef_metadata != null) && (ef_metadata != JSONObject.NULL)) {
			
			// For JSON Solr format see:
			//   https://cwiki.apache.org/confluence/display/solr/Uploading+Data+with+Index+Handlers
				
			//String title= ef_metadata.getString("title");
			JSONObject solr_add_json = new JSONObject();
			
			JSONObject solr_doc_json = generateMetadataSolrDocJSON(volume_id,ef_metadata,false);
			
			solr_add_json.put("commitWithin", 60000); // used to be 5000
			solr_add_json.put("doc", solr_doc_json);

			solr_update_json = new JSONObject();
			solr_update_json.put("add",solr_add_json);
		
		}
		else {
			System.err.println("Warning: null metadata for '" + volume_id + "'");
		}

		return solr_update_json;
	}
	
	protected static ArrayList<String> getTokenPosCountWords(JSONObject ef_token_pos_count, String page_id,
															 boolean icu_tokenize)
	{
		boolean lowercase_filter = true;
		
	    ArrayList<String> words = new ArrayList<String>();
		
		if ((ef_token_pos_count != null) && (ef_token_pos_count != JSONObject.NULL)) {

			Iterator<String> word_token_iter = ef_token_pos_count.keys();
			while (word_token_iter.hasNext()) {
				String word_token = word_token_iter.next();
				
				if (icu_tokenize) {
					Reader reader = new StringReader(word_token);
					
					ICUTokenizer icu_tokenizer = new ICUTokenizer();
					icu_tokenizer.setReader(reader);
						
					CharTermAttribute charTermAttribute = icu_tokenizer.addAttribute(CharTermAttribute.class);

					TokenStream token_stream = null;
					
					if (lowercase_filter) {
				    	token_stream = new LowerCaseFilter(icu_tokenizer);
				    }
				    else {
				    	token_stream = icu_tokenizer;
				    }
					
					try {
						token_stream.reset();
						
						while (token_stream.incrementToken()) {
						    String term = charTermAttribute.toString();
						    words.add(term);
						}
						
						token_stream.end();
						token_stream.close();
					} 
					catch (IOException e) {
						e.printStackTrace();
					}
				}
				else {
					words.add(word_token);
				}
			}				
		}
		else {
			System.err.println("Warning: empty tokenPosCount field for '" + page_id + "'");
		}
		
		/* Alternative way to get at keys
		Set<String> token_keys = ef_token_pos_count.keySet();
		for (String token : token_keys) {
				sb.append(token + " ");
		}
*/
		return words;
	}
	
	protected static ArrayList<POSString> getTokenPosCountWordsArrayList(JSONObject ef_token_pos_count, String page_id,
			boolean icu_tokenize)
	{
		ArrayList<POSString> words = new ArrayList<POSString>();

		if (ef_token_pos_count != null) {

			Iterator<String> word_token_iter = ef_token_pos_count.keys();
			while (word_token_iter.hasNext()) {
				String word_token = word_token_iter.next();

				JSONObject pos_json_object = ef_token_pos_count.getJSONObject(word_token);
				
				Set<String> pos_keys = pos_json_object.keySet();	
				int pos_keys_len = pos_keys.size();
				String[] pos_tags = (pos_keys_len>0) ? pos_keys.toArray(new String[pos_keys_len]) : null;
				
				if (icu_tokenize == true) {
					Reader reader = new StringReader(word_token);

					ICUTokenizer icu_tokenizer = new ICUTokenizer();
					icu_tokenizer.setReader(reader);

					CharTermAttribute charTermAttribute = icu_tokenizer.addAttribute(CharTermAttribute.class);

					TokenStream token_stream = icu_tokenizer;

					try {
						token_stream.reset();

						while (token_stream.incrementToken()) {
							String term = charTermAttribute.toString();
							
							POSString pos_string = new POSString(term,pos_tags);
						
							words.add(pos_string);
						}

						token_stream.end();
						token_stream.close();
					} 
					catch (IOException e) {
						e.printStackTrace();
					}
				}
				else {
					POSString pos_word_token = new POSString(word_token,pos_tags);
					
					words.add(pos_word_token);
				}
			}				
		}
		else {
			System.err.println("Warning: empty tokenPosCount field for '" + page_id + "'");
		}

		return words;
	}
	protected static ArrayList<POSString> getTokenPosCountWordsMapCaseInsensitive(ArrayList<POSString> words_in)
	{
		ArrayList<POSString> words_out = new ArrayList<POSString>();

		for (POSString pos_word: words_in) {
			String word = pos_word.getString();
			String[] pos_tags = pos_word.getPOSTags();
			
			Reader reader = new StringReader(word);
			
			Tokenizer tokenizer = new StandardTokenizer();			
			tokenizer.setReader(reader);
			CharTermAttribute charTermAttribute = tokenizer.addAttribute(CharTermAttribute.class);

			TokenStream token_stream = new LowerCaseFilter(tokenizer);
			
			try {
				token_stream.reset();

				while (token_stream.incrementToken()) {
					String term = charTermAttribute.toString();
					
					POSString pos_term = new POSString(term,pos_tags);
					words_out.add(pos_term);
				}

				token_stream.end();
				token_stream.close();
			} 
			catch (IOException e) {
				e.printStackTrace();
			}		

		}
	
		return words_out;
	}
	
	protected static ArrayList<String> lowerCaseTerms(String word)
	{
		ArrayList<String> words_out = new ArrayList<String>();
		
		Reader reader = new StringReader(word);
		
		Tokenizer tokenizer = new StandardTokenizer();			
		tokenizer.setReader(reader);
		CharTermAttribute charTermAttribute = tokenizer.addAttribute(CharTermAttribute.class);

		TokenStream token_stream = new LowerCaseFilter(tokenizer);
		
		try {
			token_stream.reset();

			while (token_stream.incrementToken()) {
				String term = charTermAttribute.toString();
				
				words_out.add(term);
			}

			token_stream.end();
			token_stream.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}		
		
		return words_out;
	}
	
	protected static ArrayList<POSString> getTokenPosCountWordsMapWhitelist(ArrayList<POSString> words_in,
																	     WhitelistBloomFilter whitelist_bloomfilter)
	{
		ArrayList<POSString> pos_words_out = new ArrayList<POSString>();

		for (POSString pos_word: words_in) {
			String word = pos_word.getString();
			String[] pos_tags = pos_word.getPOSTags();
			
			if (whitelist_bloomfilter.contains(word)) {
							
				ArrayList<String> word_terms = lowerCaseTerms(word);
				for (String term: word_terms) {
					POSString pos_term = new POSString(term, pos_tags);
					
					pos_words_out.add(pos_term);
				}
				
				// The old, direct way of adding the value in
				//pos_words_out.add(pos_word);
			}
			else {
				// else clause won't happen so often 
				//   (has to be an 'obscure' word *not* be in the whitelist to get here)
				// break down the word into terms, and see if any of them are in the whitelist instead
				
				ArrayList<String> word_terms = lowerCaseTerms(word);
				for (String term: word_terms) {
					
					if (whitelist_bloomfilter.contains(term)) {
						POSString pos_term = new POSString(term, pos_tags);
						
						pos_words_out.add(pos_term);
					}
				}
				
			
			}
		}
		
		return pos_words_out;
	}
	
	protected static ArrayList<String> getTokenPosCountPOSLabels(JSONObject ef_token_pos_count, String page_id)
	{
		ArrayList<String> pos_labels = new ArrayList<String>();

		if ((ef_token_pos_count != null) && (ef_token_pos_count != JSONObject.NULL)) {

			Iterator<String> word_token_iter = ef_token_pos_count.keys();
			while (word_token_iter.hasNext()) {
				String word_token = word_token_iter.next();
				
				JSONObject word_pos_labels = ef_token_pos_count.getJSONObject(word_token);
				
				Iterator<String> pos_token_iter = word_pos_labels.keys();
				while (pos_token_iter.hasNext()) {
					String pos_token = pos_token_iter.next();
						
					pos_labels.add(pos_token);
				}
			}				
		}
		else {
			System.err.println("Warning: empty tokenPosCount field for '" + page_id + "'");
		}

		return pos_labels;
	}

	
	
	protected static String generateSolrText(JSONObject ef_token_pos_count, String page_id,
											WhitelistBloomFilter whitelist_bloomfilter, boolean icu_tokenize)
	{
		ArrayList<String> tokens = getTokenPosCountWords(ef_token_pos_count, page_id,icu_tokenize);

		StringBuilder sb = new StringBuilder();
	
		if (whitelist_bloomfilter == null) {

			boolean first_append = true;

			for (int i=0; i<tokens.size(); i++) {
				String token = tokens.get(i);

				if (!first_append) {
					sb.append(" ");
				}
				else {
					first_append = false;
				}
				sb.append(token);
			}
		}
		else {
			boolean first_append = true;

			for (int i=0; i<tokens.size(); i++) {
				String token = tokens.get(i);

				if (whitelist_bloomfilter.contains(token)) {
					if (!first_append) {
						sb.append(" ");
					}
					else {
						first_append = false;
					}
					sb.append(token);
				}					
			}

		}


		return sb.toString();
	}
	
	protected static ArrayList<POSString> filterSolrTextFields(JSONObject ef_token_pos_count, String page_id,
											   WhitelistBloomFilter whitelist_bloomfilter, 
											   UniversalPOSLangMap universal_langmap,
											   boolean icu_tokenize)
	{
		ArrayList<POSString> cs_tokens = getTokenPosCountWordsArrayList(ef_token_pos_count, page_id,icu_tokenize);
		//ArrayList<POSString> lc_tokens = getTokenPosCountWordsMapCaseInsensitive(cs_tokens);
		
		ArrayList<POSString> tokens = null;
		if (whitelist_bloomfilter != null) {
			tokens =  getTokenPosCountWordsMapWhitelist(cs_tokens,whitelist_bloomfilter);
			//tokens =  getTokenPosCountWordsMapWhitelist(lc_tokens,whitelist_bloomfilter);
		}
		else {
			ArrayList<POSString> lc_tokens = getTokenPosCountWordsMapCaseInsensitive(cs_tokens);
			tokens = lc_tokens;
		}

		return tokens;
	}

	protected static void addToSolrLanguageTextFieldMap(HashMap<String,JSONArray> pos_lang_text_field_map,
														String pos_lang_text_field, String text_value)
	{
		if (!pos_lang_text_field_map.containsKey(pos_lang_text_field)) {
			JSONArray empty_json_values = new JSONArray();
			pos_lang_text_field_map.put(pos_lang_text_field, empty_json_values);
		}
		pos_lang_text_field_map.get(pos_lang_text_field).put(text_value);
	}

    	protected abstract void addSolrLanguageTextFields(JSONObject ef_page, ArrayList<POSString> text_al,
													UniversalPOSLangMap universal_langmap,
							  JSONObject solr_doc_json);

	
	protected static JSONObject explicitCommitWithin()
	{
		JSONObject solr_add_json = new JSONObject();	
		solr_add_json.put("commitWithin", 60000); // used to be 5000
		
		JSONObject solr_update_json = new JSONObject();
	
		return solr_update_json;
	}
	
	protected JSONObject generateSolrDocJSON(String volume_id, String page_id, 
												    JSONObject ef_metadata, JSONObject ef_page,
											        WhitelistBloomFilter whitelist_bloomfilter, 
											        UniversalPOSLangMap universal_langmap,
											        boolean icu_tokenize) 
	{
		JSONObject solr_update_json = null;
		
		if (ef_page != null) {
			
			if (!ef_page.isNull("body")) {
				JSONObject ef_body = ef_page.getJSONObject("body");
				
				
				if (!ef_body.isNull("tokenPosCount")) {
					JSONObject ef_token_pos_count = ef_body.getJSONObject("tokenPosCount");
					
					JSONObject solr_add_json = new JSONObject();
					
					ArrayList<POSString> text_al = filterSolrTextFields(ef_token_pos_count,page_id,whitelist_bloomfilter,universal_langmap,icu_tokenize);
					
					//JSONObject solr_doc_json = new JSONObject();
					JSONObject solr_doc_json = generateMetadataSolrDocJSON(page_id,ef_metadata,true);
					
					//solr_doc_json.put("id", page_id); // now done in generateMetadataSolrDocJSON
					solr_doc_json.put("volumeid_s", volume_id);
					
					if (text_al.size()>0) {
						addSolrLanguageTextFields(ef_page,text_al, universal_langmap, solr_doc_json);
						//solr_doc_json.put("eftext_txt", text_al.toString()); // ****
					}
					else {
						solr_doc_json.put("efnotext_b", true);
					}
					solr_add_json.put("commitWithin", 60000); // used to be 5000
					solr_add_json.put("doc", solr_doc_json);
					
					solr_update_json = new JSONObject();
					solr_update_json.put("add",solr_add_json);
					
				}
				else {
					System.err.println("Warning: empty tokenPosCount field for '" + page_id + "'");
				}
			}
			// No need to print out warning in 'body' null
			// Used to signify that the page had no detected text as the body			
		}
		else {
			System.err.println("Warning: null page for '" + page_id + "'");
		}
	    /*
	     /update/json/docs
	     */
	    
		// For Reference ...
		// Example documentation on Solr JSON syntax:
		//   https://cwiki.apache.org/confluence/display/solr/Uploading+Data+with+Index+Handlers
		//     #UploadingDatawithIndexHandlers-JSONFormattedIndexUpdates
		
		/*
		curl -X POST -H 'Content-Type: application/json' 'http://localhost:8983/solr/my_collection/update' --data-binary '
		{
		  "add": {
		    "doc": {
		      "id": "DOC1",
		      "my_boosted_field": {         use a map with boost/value for a boosted field 
		        "boost": 2.3,
		        "value": "test"
		      },
		      "my_multivalued_field": [ "aaa", "bbb" ]    Can use an array for a multi-valued field 
		    }
		  },
		  "add": {
		    "commitWithin": 5000,           commit this document within 5 seconds 
		    "overwrite": false,             don't check for existing documents with the same uniqueKey 
		    "boost": 3.45,                  a document boost 
		    "doc": {
		      "f1": "v1",                   Can use repeated keys for a multi-valued field 
		      "f1": "v2"
		    }
		  },
		 
		  "commit": {},
		  "optimize": { "waitSearcher":false },
		 
		  "delete": { "id":"ID" },          delete by ID 
		  "delete": { "query":"QUERY" }     delete by query 
		}'
		*/
		
		return solr_update_json;
	}
	
	public static void convertCapiscoConceptsPageTOSolrMetadata(JSONObject page_rec, JSONArray concept_vals_array )
	{	
		if (page_rec.has("concepts")) {
			JSONArray concept_rec_array = page_rec.getJSONArray("concepts");
			int concept_rec_array_len = concept_rec_array.length();

			for (int c=0; c<concept_rec_array_len; c++) {
				JSONObject concept_rec = concept_rec_array.getJSONObject(c);

				if (concept_rec.has("text")) {
					String concept_text = concept_rec.getString("text");
					concept_vals_array.put(concept_text);
					
					//String concept_text_unicode = StringEscapeUtils.unescapeJava(concept_text);
					//concept_vals_array.put(concept_text_unicode);
				}
			}
		}
	}
	
	public static JSONArray convertCapiscoConceptsPageTOSolrMetadata(JSONObject page_rec)
	{
		JSONArray concept_vals_array = new JSONArray();
		convertCapiscoConceptsPageTOSolrMetadata(page_rec,concept_vals_array );
		return concept_vals_array;
	}
	
	public static JSONObject generateSolrUpdateMetadata(String id, JSONObject metadata,  
														boolean is_page_level, String update_mode)
	{
		JSONObject update_field_keys = new JSONObject();
		update_field_keys.put("id",id);
		
		Iterator<String> metadata_key_iter = metadata.keys();
		while (metadata_key_iter.hasNext()) {
			String metadata_key = metadata_key_iter.next();
			
			// Example key entry:
			//   key : {"add-distinct": [vals]} 
			
			// where 'add-distinct' would be defined in update_mode;
			
			JSONArray field_vals = metadata.getJSONArray(metadata_key);
			JSONObject field_update_mode = new JSONObject();
			field_update_mode.put(update_mode, field_vals);
			
			if (is_page_level) {
			    update_field_keys.put("volume"+metadata_key+"_txt",field_update_mode);
				update_field_keys.put("volume"+metadata_key+"_htrcstrings",field_update_mode);
			}
			else {
				update_field_keys.put(metadata_key+"_t",field_update_mode);
				update_field_keys.put(metadata_key+"_ss",field_update_mode);
			}
		}
		
		return update_field_keys;
	}
	 	
	public static JSONArray generateIncrementalPageUpdateMetadata(String volume_id, String page_id, JSONObject page_rec)
	{
		// Looking to generate JSON in the form:
		//
		//{ "id":"mydoc",
		//	 "price":{"set":99},
		//	 "popularity":{"inc":20},
		//	 "categories":{"add":["toys","games"]},
		//	 "sub_categories":{"add-distinct":"under_10"},
		//	 "promo_ids":{"remove":"a123x"},
		//	 "tags":{"remove":["free_to_try","on_sale"]}
		//	}
		//
		// (from https://lucene.apache.org/solr/guide/7_3/updating-parts-of-documents.html)
		
		// Extract out the concepts we want and turn into JSONArray of vals
		JSONArray page_concept_vals_array = convertCapiscoConceptsPageTOSolrMetadata(page_rec);

		JSONArray update_field_keys = null;
		
		if (page_concept_vals_array.length() > 0) {
			
			JSONObject metadata = new JSONObject();
			metadata.put("concept", page_concept_vals_array); 

			// Now generate the JSONObject for Solr, both for the page level and as a top-up  to the volume-level
			String full_page_id = volume_id+"."+page_id;
			//JSONObject page_update_field_keys = generateSolrUpdateMetadata(full_page_id,metadata,true,"add-distinct"); // is_page_level=true
			JSONObject page_update_field_keys_del = generateSolrUpdateMetadata(full_page_id,metadata,true,"remove");     // is_page_level=true
			JSONObject page_update_field_keys_add = generateSolrUpdateMetadata(full_page_id,metadata,true,"add");        // is_page_level=true

			//JSONObject vol_update_field_keys = generateSolrUpdateMetadata(volume_id,metadata,false,"add-distinct"); // is_page_level=false
			JSONObject vol_update_field_keys_del = generateSolrUpdateMetadata(volume_id,metadata,false,"remove");     // is_page_level=false
			JSONObject vol_update_field_keys_add = generateSolrUpdateMetadata(volume_id,metadata,false,"add");        // is_page_level=false

			update_field_keys = new JSONArray();
			//update_field_keys.put(page_update_field_keys);
			update_field_keys.put(page_update_field_keys_del);
			update_field_keys.put(page_update_field_keys_add);

			//update_field_keys.put(vol_update_field_keys);
			update_field_keys.put(vol_update_field_keys_del);
			update_field_keys.put(vol_update_field_keys_add);
			
			//solr_add_json.put("commitWithin", 60000); // used to be 5000
		}
		
		return update_field_keys;
	}
	
	public static JSONArray generateIncrementalVolumeUpdateMetadata(String volume_id, JSONArray pages_array)
	{
		
		// update_mode: "set", "inc", "add", "remove", ...
		
		// Looking to generate JSON in the form:
		//
		//{ "id":"mydoc",
		//	 "price":{"set":99},
		//	 "popularity":{"inc":20},
		//	 "categories":{"add":["toys","games"]},
		//	 "sub_categories":{"add-distinct":"under_10"},
		//	 "promo_ids":{"remove":"a123x"},
		//	 "tags":{"remove":["free_to_try","on_sale"]}
		//	}
		//
		// (from https://lucene.apache.org/solr/guide/7_3/updating-parts-of-documents.html)
		
		// Extract out the concepts we want and turn into JSONArray of vals
		int pages_array_len = pages_array.length();

		JSONArray volume_concept_vals_array = new JSONArray();

		for (int p=0; p<pages_array_len; p++) {

			JSONObject page_rec = pages_array.getJSONObject(p);
			convertCapiscoConceptsPageTOSolrMetadata(page_rec,volume_concept_vals_array);
		}
			
		JSONObject metadata = new JSONObject();
		metadata.put("concept", volume_concept_vals_array); 
		
		// Now generate the JSONObject for Solr
		boolean is_page_level = false; // for readability
		//JSONObject update_field_keys = generateSolrUpdateMetadata(volume_id,metadata,is_page_level,"add-distinct");
		JSONObject update_field_keys_del = generateSolrUpdateMetadata(volume_id,metadata,is_page_level,"remove"); 
		JSONObject update_field_keys_add = generateSolrUpdateMetadata(volume_id,metadata,is_page_level,"add"); 
		
		JSONArray update_field_keys = new JSONArray();
		update_field_keys.put(update_field_keys_del);
		update_field_keys.put(update_field_keys_add);
		
		return update_field_keys;
	}
	
	public static ArrayList<String> generateTokenPosCountWhitelistText(String volume_id, String page_id, JSONObject ef_page,
															  boolean icu_tokenize) 
	{
		ArrayList<String> word_list = null;
		
		if (ef_page != null) {
			
			if (!ef_page.isNull("body")) {
				JSONObject ef_body = ef_page.getJSONObject("body");
				
				JSONObject ef_token_pos_count = ef_body.optJSONObject("tokenPosCount");
				word_list = getTokenPosCountWords(ef_token_pos_count,page_id,icu_tokenize);
			}
			// No need to print out warning in 'body' null
			// Used to signify that the page had no detected text as the body
		}
		else {
			System.err.println("Warning: null page for '" + page_id + "'");
		}
		
		return word_list;
	}
	
	public static ArrayList<String> generateTokenPosCountPOSLabels(String volume_id, String page_id, JSONObject ef_page) 
	{
		ArrayList<String> word_list = null;

		if (ef_page != null) {
			
			if (!ef_page.isNull("body")) {
				JSONObject ef_body = ef_page.getJSONObject("body");
				
				JSONObject ef_token_pos_count = ef_body.optJSONObject("tokenPosCount");
				word_list = getTokenPosCountPOSLabels(ef_token_pos_count,page_id);
			}
			// No need to print out warning in 'body' null
			// Used to signify that the page had no detected text as the body

		}
		else {
			System.err.println("Warning: null page for '" + page_id + "'");
		}

		return word_list;
	}
	
	public abstract ArrayList<String> generateTokenPosCountLangLabels(String volume_id, String page_id, JSONObject ef_page);
	
	public static void saveSolrDoc(JSONObject solr_add_doc_json, String output_file_json_bz2)
	{
		try {
			BufferedWriter bw = ClusterFileIO.getBufferedWriterForCompressedFile(output_file_json_bz2);
			bw.write(solr_add_doc_json.toString());
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (CompressorException e) {
			e.printStackTrace();
		}
	}
	
	public static ArrayList<String > generateRandomRetrySolrEnpoints(ArrayList<String> solr_endpoints, int num_retries)
	{
		ArrayList<String> solr_url_alts = null;
		
		int solr_endpoints_len = solr_endpoints.size();
		if (solr_endpoints_len > 0) {
			solr_url_alts = new ArrayList<String>();
			for (int i=0; i<num_retries; i++) {
				int random_choice = (int)(solr_endpoints_len * Math.random());
				String solr_url = solr_endpoints.get(random_choice);
				solr_url_alts.add(solr_url);
			}
		}
		
		System.err.println("**** generateRandomRetrySolrEnpoints() returning: " + solr_url_alts);
		return solr_url_alts;
	}
	
	public static ArrayList<String > generateRandomRetrySolrEnpoints(ArrayList<String> solr_endpoints)
	{
		return generateRandomRetrySolrEnpoints(solr_endpoints,NUM_ALT_RETRIES);
	}
	
	public static HttpURLConnection openConnectionWithRetries(ArrayList<String> post_url_alts)
	{
		HttpURLConnection successful_httpcon = null;
		
		System.err.println("****** openConnectionWithRetries() post_url_alts = " + post_url_alts);
		
		try { 
			String first_post_url_str = post_url_alts.get(0);
			URL first_post_url = new URL(first_post_url_str);
			HttpURLConnection first_httpcon = (HttpURLConnection) (first_post_url.openConnection());

			int first_response_code = first_httpcon.getResponseCode();
			if (first_response_code == HttpURLConnection.HTTP_UNAVAILABLE) {
				System.err.println("Warning: HTTP_UNAVAILABLE (response code: "+first_response_code+") connecting to "+first_post_url_str);
				first_httpcon.disconnect();
				
				String prev_post_url_str = first_post_url_str;
				post_url_alts.remove(0);
				
				boolean retry_successful = false;

				while (post_url_alts.size()>0) {
				
					String alt_post_url_str = post_url_alts.get(0);
					URL alt_post_url = new URL(alt_post_url_str);

					long random_msec = (long) (2000 + (2000 * Math.random())); // 2-4 secs delay
					String mess = "         Sleeping for "+random_msec+" msecs, then trying again";
					if (!alt_post_url_str.equals(prev_post_url_str)) {
						mess += " (with different Solr endpoint)";
					}
					System.out.println(mess);

					try {
						Thread.sleep(random_msec);
					}
					catch (InterruptedException e) {
						e.printStackTrace();
						break;
					}
					HttpURLConnection alt_httpcon = (HttpURLConnection)(alt_post_url.openConnection());
					
					int alt_response_code = alt_httpcon.getResponseCode();
					if (alt_response_code == HttpURLConnection.HTTP_OK) {
						successful_httpcon = alt_httpcon;
						retry_successful = true;
						break;
					}

					System.out.println("Warning: HTTP_UNAVAILABLE (response code: "+alt_response_code+") connecting to "+alt_post_url_str);
					alt_httpcon.disconnect();
					
					prev_post_url_str = alt_post_url_str;
					post_url_alts.remove(0);
				}

				if (retry_successful) {
					System.err.println("Retry successful");
				}
				else {
					System.err.println("**** Retry NOT successful!");
				}
			}
			else {
				successful_httpcon = first_httpcon;
			}
		}
		catch (MalformedURLException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		return successful_httpcon;
	}
	
	public static void postSolrDoc(ArrayList<String> post_url_alts, String solr_add_doc_json_str,
				       				   String info_volume_id, String info_page_id)
	{
		
		//String curl_popen = "curl -X POST -H 'Content-Type: application/json'";
		//curl_popen += " 'http://10.11.0.53:8983/solr/htrc-pd-ef/update'";
		//curl_popen += " --data-binary '";
		//curl_popen += "'"


	        // System.out.println("Post URL: " + post_url);
		
		// Consider breaking into version with support method to create connection, along
		// similar lines to:
		//   https://codereview.stackexchange.com/questions/45819/httpurlconnection-response-code-handling
		
		try {
			
			//String post_url_str = post_url_alts.remove(0);
			//URL post_url = new URL(post_url_str);
			//HttpURLConnection httpcon = (HttpURLConnection) (post_url.openConnection());
			
			HttpURLConnection httpcon = openConnectionWithRetries(post_url_alts);
			
			//httpcon.setDoInput(true);	
			//httpcon.setDoOutput(true);				
			
			// Basic Realm authentication based on:
			//   https://www.baeldung.com/java-http-url-connection
			// Consider moving away from Aapche Commons Base64 and use Java8 one??
			String user = "admin";
			String password = null;
			
			if (password != null) {
			    String auth = user + ":" + password;
			    byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
			    String authHeaderValue = "Basic " + new String(encodedAuth);
			    httpcon.setRequestProperty("Authorization", authHeaderValue);
			}
			
			httpcon.setRequestProperty("Content-Type", "application/json");
			httpcon.setRequestProperty("Accept", "application/json");
			httpcon.setRequestMethod("POST");
			httpcon.connect();

			byte[] outputBytes = solr_add_doc_json_str.getBytes("UTF-8");
			OutputStream os = httpcon.getOutputStream();
			os.write(outputBytes);
			os.close();
			
			
			// Read response
			StringBuilder sb = new StringBuilder();
			InputStream is = httpcon.getInputStream();
			BufferedReader in = new BufferedReader(new InputStreamReader(is));
			String decodedString;
			while ((decodedString = in.readLine()) != null) {
				sb.append(decodedString);
			}
			in.close();
			httpcon.disconnect();
			
			JSONObject solr_status_json = new JSONObject(sb.toString());
			
			if (!solr_status_json.isNull("responseHeader")) {
				JSONObject response_header_json = solr_status_json.getJSONObject("responseHeader");
				
				int status = response_header_json.getInt("status");
				if (status != 0) {
					System.err.println("Warning: POST request to " + post_url_alts.get(0) + " returned status " + status);
					System.err.println("Full response was: " + sb);
				}
			}
			else {
				System.err.println("Failed response to Solr POST: " + sb);
			}
			
			
			
		}
		catch (IOException e) {
		        System.err.println("Solr core update failed when processing id: " + info_page_id);
		        System.err.println("Solr Doc posted for ingest was:\n" + solr_add_doc_json_str);
		        
		        e.printStackTrace();
		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void postSolrDoc(ArrayList<String> post_url_alts, JSONObject solr_add_doc_json,
			String info_volume_id, String info_page_id)
	{
		postSolrDoc(post_url_alts,solr_add_doc_json.toString(),info_volume_id,info_page_id);
	}

	public static void postSolrDoc(ArrayList<String> post_url_alts, JSONArray solr_add_doc_json_array,
			String info_volume_id, String info_page_id)
	{
		postSolrDoc(post_url_alts,solr_add_doc_json_array.toString(),info_volume_id,info_page_id);
	}

}
