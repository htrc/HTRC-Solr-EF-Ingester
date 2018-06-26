package org.hathitrust.extractedfeatures;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.compress.compressors.CompressorException;
import org.json.JSONArray;
import org.json.JSONObject;

import scala.Tuple2;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.core.LowerCaseFilter;

public class SolrDocJSON {

	protected static String [] metadata_single = new String[] {
			"accessProfile",
			"bibliographicFormat",
			"dateCreated",              // date
			//"enumerationChronology",  // What is this?
			//"governmentDocument",       // bool: true/false
			"handleUrl",
			"hathitrustRecordNumber",   // int?
			"htBibUrl",
			"imprint",
			"issuance",
			"language",
			"lastUpdateDate",
			"pubDate",
			"pubPlace",
			"rightsAttributes",
			"schemaVersion",
			"sourceInstitution",
			"sourceInstitutionRecordNumber",
			"title",
			"typeOfResource",
			"volumeIdentifier"
	};

	protected static String [] metadata_multiple = new String[] {
			"oclc",
			"isbn",
			"issn",
			"lccn",
			"genre", 	    
			"names"
	};

	protected static String [] metadata_hashmap_multiple = new String[] {
			"classification"
	};
	
	protected static JSONObject generateMetadataSolrDocJSON(String id, JSONObject ef_metadata, boolean is_page_level)
	{
		/*
		 Example JSON for id: "gri.ark:/13960/t0003qw46
		 metadata: {
		 
		"accessProfile": "open",
        "bibliographicFormat": "BK",
        "classification": {
            "lcc": [
                "ND646 .B8 1900"
            ]
        },
        "dateCreated": "2016-06-19T08:30:16.11199Z",
        "enumerationChronology": " ",
        "genre": [
            "not fiction"
        ],
        "governmentDocument": false,
        "handleUrl": "http://hdl.handle.net/2027/gri.ark:/13960/t0003qw46",
        "hathitrustRecordNumber": "100789562",
        "htBibUrl": "http://catalog.hathitrust.org/api/volumes/full/htid/gri.ark:/13960/t0003qw46.json",
        "imprint": "Burlington Fine Arts Club, 1900.",
        "isbn": [],
        "issn": [],
        "issuance": "monographic",
        "language": "eng",
        "lastUpdateDate": "2015-09-14 13:25:03",
        "lccn": [],
        "names": [
            "Burlington Fine Arts Club "
        ],
        "oclc": [
            "25259734"
        ],
        "pubDate": "1900",
        "pubPlace": "enk",
        "rightsAttributes": "pd",
        "schemaVersion": "1.3",
        "sourceInstitution": "CMALG",
        "sourceInstitutionRecordNumber": "9928077890001551",
        "title": "Exhibition of pictures by Dutch masters of the seventeenth century.",
        "typeOfResource": "text",
        "volumeIdentifier": "gri.ark:/13960/t0003qw46"

          }
		 
		 */
		

		// For JSON Solr format see:
		//   https://cwiki.apache.org/confluence/display/solr/Uploading+Data+with+Index+Handlers

		//String title= ef_metadata.getString("title");

		JSONObject solr_doc_json = new JSONObject();
		solr_doc_json.put("id", id);

		for (String metaname: metadata_single) {
			String metavalue = ef_metadata.getString(metaname);
			
			if (metavalue != null) {
				if (is_page_level) {
					solr_doc_json.put("volume"+metaname+"_txt",metavalue);
					solr_doc_json.put("volume"+metaname+"_htrcstring",metavalue);
				}
				else {
					solr_doc_json.put(metaname+"_t",metavalue);
					solr_doc_json.put(metaname+"_s",metavalue);
				}
			}
		}

		for (String metaname: metadata_multiple) {
			JSONArray metavalues = ef_metadata.getJSONArray(metaname);
			if (metavalues != null) {
				if (is_page_level) {
					solr_doc_json.put("volume"+metaname+"_txt",metavalues);
					solr_doc_json.put("volume"+metaname+"_htrcstrings",metavalues);
				}
				else {
					solr_doc_json.put(metaname+"_t",metavalues);
					solr_doc_json.put(metaname+"_ss",metavalues);
				}
			}
		}

		for (String metaname: metadata_hashmap_multiple) {
			JSONObject metakeys = ef_metadata.getJSONObject(metaname);

			if (metakeys != null) {
				
				Iterator<String> metakey_iter = metakeys.keys();
				while (metakey_iter.hasNext()) {
					String metakey = metakey_iter.next();

					JSONArray metavalues = metakeys.getJSONArray(metakey);
					if (metavalues != null) {
						String combined_metaname = metaname + "_" + metakey;
						if (is_page_level) {
							solr_doc_json.put("volume"+combined_metaname+"_txt",metavalues);
							solr_doc_json.put("volume"+combined_metaname+"_htrcstrings",metavalues);
						}
						else {
							solr_doc_json.put(combined_metaname+"_t",metavalues);
							solr_doc_json.put(combined_metaname+"_ss",metavalues);
						}
					}	
				}
			}
		}

		return solr_doc_json;

	}
	
	protected static JSONObject generateToplevelMetadataSolrDocJSON(String volume_id, JSONObject ef_metadata)
	{
		JSONObject solr_update_json = null;
		
		if (ef_metadata != null) {
			
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
		
		if (ef_token_pos_count != null) {

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

		if (ef_token_pos_count != null) {

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

	protected static void addSolrLanguageTextFields(JSONObject ef_page, ArrayList<POSString> text_al,
													UniversalPOSLangMap universal_langmap,
												    JSONObject solr_doc_json)
	{
		// e.g. ... "languages":[{"ko":"0.71"},{"ja":"0.29"}]
		JSONArray ef_languages = ef_page.getJSONArray("languages");
		if ((ef_languages != null) && (ef_languages.length()>0)) {
			
			int lang_len = ef_languages.length();
			String [] lang_list = new String[lang_len];
			
			for (int i=0; i<lang_len; i++) {
				JSONObject lang_rec = ef_languages.getJSONObject(i);

				Iterator<String> lang_key_iter = lang_rec.keys();
				while (lang_key_iter.hasNext()) {
					String lang_label = lang_key_iter.next();

					lang_list[i] = lang_label;
				}
			}
			
			int text_len = text_al.size();
			
			/*
			for (int li=0; li<lang_len; li++) {
				String lang_key = lang_list[li];
				
				if (universal_langmap.containsLanguage(lang_key))
				{
				*/
					HashMap<String,JSONArray> pos_lang_text_field_map = new HashMap<String,JSONArray>();
					
					for (int ti=0; ti<text_len; ti++) {
						POSString pos_text_value = text_al.get(ti);
						String text_value = pos_text_value.getString();
						
						String[] pos_tags = pos_text_value.getPOSTags();
						int pos_tags_len = pos_tags.length;
						
						for (int pti=0; pti<pos_tags_len; pti++) {
							String opennlp_pos_key = pos_tags[pti];
							
							Tuple2<String,String> lang_pos_pair = universal_langmap.getUniversalLanguagePOSPair(lang_list, opennlp_pos_key);
							String selected_lang = lang_pos_pair._1;
							String upos = lang_pos_pair._2;
							
							String pos_lang_text_field = selected_lang;
							if (upos != null) {
								pos_lang_text_field += "_" + upos; 
							}
							pos_lang_text_field += "_htrctokentext";
							
							if (!pos_lang_text_field_map.containsKey(pos_lang_text_field)) {
								JSONArray empty_json_values = new JSONArray();
								pos_lang_text_field_map.put(pos_lang_text_field, empty_json_values);
							}
							pos_lang_text_field_map.get(pos_lang_text_field).put(text_value);
						}
					}

					// Now add each of the POS language fields into solr_doc_json
					Set<String> pos_lang_field_keys = pos_lang_text_field_map.keySet();
					for (String plf_key : pos_lang_field_keys) {
						String lang_text_field = plf_key;
						JSONArray json_values = pos_lang_text_field_map.get(plf_key);
						
						solr_doc_json.put(lang_text_field, json_values); 
					}
					/*
				}
				else {
					String lang_text_field = lang_key + "_htrctokentext";
					
					JSONArray json_values = new JSONArray();
					for (int ti=0; ti<text_len; ti++) {
						POSString pos_text_value = text_al.get(ti);
						String text_value = pos_text_value.getString();
						json_values.put(text_value);
					}
					solr_doc_json.put(lang_text_field, json_values); 
					
				}
				
				
			}
			*/
		}
	}
	
	protected static JSONObject generateSolrDocJSON(String volume_id, String page_id, 
												    JSONObject ef_metadata, JSONObject ef_page,
											        WhitelistBloomFilter whitelist_bloomfilter, 
											        UniversalPOSLangMap universal_langmap,
											        boolean icu_tokenize) 
	{
		JSONObject solr_update_json = null;
		
		if (ef_page != null) {
			JSONObject ef_body = ef_page.getJSONObject("body");
			if (ef_body != null) {
				JSONObject ef_token_pos_count = ef_body.getJSONObject("tokenPosCount");
				if (ef_token_pos_count != null) {
	
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
			else {
				System.err.println("Warning: empty body field for '" + page_id + "'");
			}
			
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
		JSONArray concept_rec_array = page_rec.getJSONArray("concepts");
		int concept_rec_array_len = concept_rec_array.length();

		for (int c=0; c<concept_rec_array_len; c++) {
			JSONObject concept_rec = concept_rec_array.getJSONObject(c);

			if (concept_rec.has("text")) {
				String concept_text = concept_rec.getString("text");

				concept_vals_array.put(concept_text);
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
			
			// key : {"add-distinct": [vals]} 
			
			JSONArray field_vals = metadata.getJSONArray(metadata_key);
			JSONObject field_add_distinct = new JSONObject();
			field_add_distinct.put(update_mode, field_vals);
			
			if (is_page_level) {
				update_field_keys.put("volume"+metadata_key+"_txt",field_add_distinct);
				update_field_keys.put("volume"+metadata_key+"_htrcstrings",field_add_distinct);
			}
			else {
				update_field_keys.put(metadata_key+"_t",field_add_distinct);
				update_field_keys.put(metadata_key+"_ss",field_add_distinct);
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

		JSONObject metadata = new JSONObject();
		metadata.put("concept", page_concept_vals_array); 
		
		// Now generate the JSONObject for Solr, both for the page level and as a top-up  to the volume-level
		String full_page_id = volume_id+"."+page_id;
		//JSONObject page_update_field_keys = generateSolrUpdateMetadata(full_page_id,metadata,true,"add-distinct"); // is_page_level=true
		JSONObject page_update_field_keys_del = generateSolrUpdateMetadata(full_page_id,metadata,true,"remove"); // is_page_level=true
		JSONObject page_update_field_keys_add = generateSolrUpdateMetadata(full_page_id,metadata,true,"add"); // is_page_level=true
		
		//JSONObject vol_update_field_keys = generateSolrUpdateMetadata(volume_id,metadata,false,"add-distinct"); // is_page_level=false
		JSONObject vol_update_field_keys_del = generateSolrUpdateMetadata(volume_id,metadata,false,"remove"); // is_page_level=false
		JSONObject vol_update_field_keys_add = generateSolrUpdateMetadata(volume_id,metadata,false,"add"); // is_page_level=false
		
		JSONArray update_field_keys = new JSONArray();
		//update_field_keys.put(page_update_field_keys);
		update_field_keys.put(page_update_field_keys_del);
		update_field_keys.put(page_update_field_keys_add);

		//update_field_keys.put(vol_update_field_keys);
		update_field_keys.put(vol_update_field_keys_del);
		update_field_keys.put(vol_update_field_keys_add);
		
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
			JSONObject ef_body = ef_page.getJSONObject("body");
			if (ef_body != null) {
				JSONObject ef_token_pos_count = ef_body.getJSONObject("tokenPosCount");
				word_list = getTokenPosCountWords(ef_token_pos_count,page_id,icu_tokenize);
			}
			else {
				System.err.println("Warning: empty body field for '" + page_id + "'");
			}

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
			JSONObject ef_body = ef_page.getJSONObject("body");
			if (ef_body != null) {
				JSONObject ef_token_pos_count = ef_body.getJSONObject("tokenPosCount");
				word_list = getTokenPosCountPOSLabels(ef_token_pos_count,page_id);
			}
			else {
				System.err.println("Warning: empty body field for '" + page_id + "'");
			}

		}
		else {
			System.err.println("Warning: null page for '" + page_id + "'");
		}

		return word_list;
	}
	
	public static ArrayList<String> generateTokenPosCountLangLabels(String volume_id, String page_id, JSONObject ef_page) 
	{
		ArrayList<String> lang_list = new ArrayList<String>();;

		if (ef_page != null) {
			JSONArray ef_languages = ef_page.getJSONArray("languages");
			if (ef_languages != null) {
				
				int lang_len = ef_languages.length();
				for (int i=0; i<lang_len; i++) {
					JSONObject lang_rec = ef_languages.getJSONObject(i);

					Iterator<String> lang_key_iter = lang_rec.keys();
					while (lang_key_iter.hasNext()) {
						String lang_label = lang_key_iter.next();

						lang_list.add(lang_label);
					}
				}
			}
			else {
				System.err.println("Warning: empty languages field for '" + page_id + "'");
			}

		}
		else {
			System.err.println("Warning: null page for '" + page_id + "'");
		}

		return lang_list;
	}
	
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
	
        public static void postSolrDoc(String post_url, String solr_add_doc_json_str,
				       				   String info_volume_id, String info_page_id)
	{
		
		//String curl_popen = "curl -X POST -H 'Content-Type: application/json'";
		//curl_popen += " 'http://10.11.0.53:8983/solr/htrc-pd-ef/update'";
		//curl_popen += " --data-binary '";
		//curl_popen += "'"


	        // System.out.println("Post URL: " + post_url);
		
		try {
			HttpURLConnection httpcon = (HttpURLConnection) ((new URL(post_url).openConnection()));
			httpcon.setDoOutput(true);
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

			JSONObject solr_status_json = new JSONObject(sb.toString());
			JSONObject response_header_json = solr_status_json.getJSONObject("responseHeader");
			if (response_header_json != null) {
				int status = response_header_json.getInt("status");
				if (status != 0) {
					System.err.println("Warning: POST request to " + post_url + " returned status " + status);
					System.err.println("Full response was: " + sb);
				}
			}
			else {
				System.err.println("Failed response to Solr POST: " + sb);
			}
			
			
			
		}
		catch (IOException e) {
		        System.err.println("Solr core update failed when processing id: " + info_volume_id + "." + info_page_id);
			e.printStackTrace();
		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}

        public static void postSolrDoc(String post_url, JSONObject solr_add_doc_json,
				       String volume_id, String page_id)
	{
	    postSolrDoc(post_url,solr_add_doc_json.toString(),volume_id,page_id);
	}

        public static void postSolrDoc(String post_url, JSONArray solr_add_doc_json_array,
				       String volume_id, String page_id)
	{
	    postSolrDoc(post_url,solr_add_doc_json_array.toString(),volume_id,page_id);
	}
    
}
