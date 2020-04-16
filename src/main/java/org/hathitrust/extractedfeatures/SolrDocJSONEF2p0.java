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

// SolrDocJSONEF2.0 (2 point 0)
public class SolrDocJSONEF2p0 extends SolrDocJSON 
{	
	/* Mapping of changes from EF1.5 JSON format to EF2.0 */

	/*
	Record structure:
	{
	  features: {
	    id: "http://hdl.handle.net/2027/coo.31924013523232" (field wasn't previously present)
	
	   pages: [ *** largely as before, with the following notes ***
	     {
	      seq: 00000001
	      languages [] -> calculatedLanguage  (2 letter code, or null)
	      header, body, footer can be null
	     }
	    ]
	  }
	  id: -> htid
	  id: "https://data.analytics.hathitrust.org/extracted-features/20200210/coo.31924013523232"
	  
	  // The metadata field has seen the most changes
	  //
	  // The following is the result of a careful side-by-side comparison of the 
	  // same record in EF1.5 and EF2.0 formats
	  // '=' is the same in both (but value type might have changed, e.g. from string to int)
	  // '-' has been removed
	  // '+' has been added
	  // '~' has been renamed, often with significant date-format restructuring of value
	   
	  // The study was mode of 'coo.31924013523232'
	  // EF1.5: bzcat json-files-pairtree/coo/pairtree_root/31/92/40/13/52/32/32/31924013523232/coo.31924013523232.json.bz2 | python -mjson.tool | less
	  // vs
	  // EF2.0: bzcat json-files-stubby/coo/32123/coo.31924013523232.json.bz2  | python -mjson.tool | less
  
	  metadata: {
	    =accessProfile: remains unchanged
	    ~accessRights: "pd" (the new name for what was previously 'rightsAttributes')
	    ~bibliographicFormat: "BK" -> now managed as 'type' [ "DataFeedItem", "Book" ]
	  !!-classification: {} -> gone??? or it just that my example was empty/null???
	  ??contributor: {} (rdf: {id, name, type} -> replaces 'names' [], but now singular???
	    =dateCreated: "" remains, but no longer date and time, now only down to the day e.g. 20200209
	  !!=genre: [] -> "" remains, but no longer an array, now single URI (check record with multiple genre)
		-governmentDocument: false -> gone
	    ~handleUrl -> now 'id' in new format
		~hathitrustRecordNumber": "8668964" -> now appears in 'mainEntityOfPage', URI-field [0] and leading 00 padding
	    ~htBibUrl: "http://catalog.hathitrust.org/api/volumes/full/htid/coo.31924013523232.json" -> now in 'mainEntityOfPage' [2]
		~id: "http://hdl.handle.net/2027/coo.31924013523232" renamed from previous handleUrl
	    ~imprint: "B. Tauchnitz, 1873." -> now 'publisher' as RDF triple (id, name, type)
	  !!?isbn: [] -> gone or because empty???
	  !!?issn: [] -> gone or because empty???
	    -issuance: "monographic" -> gone
	    =language: remains unchanged
	  ??~lastUpdateDate: "2014-01-02 06:25:28" -> could this now be lastRightsUpdateDate: 20190603 (int)
	  ??~lastRightsUpdateDate: 20190603 -> new if not lastUpdateDate
		lccn: [] -> gone or because empty???
	    ~mainEntityOfPage: [ merging of 'hathitrustRecordNumber', and 'htBibUrl' plus the brief URI version [1]
	            "https://catalog.hathitrust.org/Record/008668964",
	            "http://catalog.hathitrust.org/api/volumes/brief/oclc/37262723.json",
	            "http://catalog.hathitrust.org/api/volumes/full/oclc/37262723.json"
	    ],
		~names: [ *** replaced by 'contributor' RDF {id, name, type} of type URI Person + viaf ID
	            "Braddon, M. E. (Mary Elizabeth) 1835-1915 "
	    ],
		=oclc: remains, but changed from [] to "", unless this is because the example only has one entry???
		=pubDate: "1873" -> 1873 (remains, but changes from string to int)
		~pubPlace: { *** remains, but changes from string "gw " to record ***
	            "id": "http://id.loc.gov/vocabulary/countries/gw",
	            "name": "Germany",
	            "type": "http://id.loc.gov/ontologies/bibframe/Place"
	    },
	    ~rightsAttributes: "pd" -> renamed to accessRights
	    ~schemaVersion: "1.3" -> "https://schemas.hathitrust.org/EF_Schema_MetadataSubSchema_v_3.0",
		~sourceInstitution: { *** remains but changes for "COO" to {}
	            "name": "COO",
	            "type": "http://id.loc.gov/ontologies/bibframe/Organization"
	        },
	    -sourceInstitutionRecordNumber: "1432129" -> gone
		=title: remains same, but looks like author subfield missed on title join/concat
	    ~type: [ -> possible renaming of Format, and change from "" to []
	            "DataFeedItem",
	            "Book"
	    ],
		~typeOfResource: "text" -> "http://id.loc.gov/ontologies/bibframe/Text" changed from string to URI
		!volumeIdentifier: "coo.31924013523232" -> gone, but is tail substring of full Handle URI version in 'id'
		
	  }
	}

	 */
	
	protected String[] metadata_single_int = null;
	protected String[] metadata_single_uri = null;
	protected String[] metadata_single_id_name_type = null;
	
	public SolrDocJSONEF2p0()
	{
		
		metadata_single_string = new String[] {
				"accessProfile",
				"accessRights", 	        // rename of 'rightsAttribute'
				//"handleUrl",			    /* now 'id' but note reason for suppression below*/ 
				//"hathitrustRecordNumber", /* now appears in 'mainEntityOfPage', URI-field [0] and leading 00 padding */
				//"htBibUrl",				/* now in 'mainEntityOfPage' URI-field [2] */
				//"issuance",			    /* removed in EF2*/

				/* We deliberately exclude 'id' from metadata_single 
				   as it is explicitly set in code from 'volume_id' param pass in */
				// "id",    	            // rename of handleUrl, 

				//"lastUpdateDate",			/* now 'lastRightsUpdateDate' */
				//"rightsAttributes",		/* now 'accessRights */
				//"sourceInstitutionRecordNumber", /* removed in EF2 */
				"title"

				//"volumeIdentifier"        /* gone, but could be auto-populated from tail substring of full Handle URI version in 'id'
		};

		metadata_single_int = new String[] {
				"dateCreated",              // changes from string to int
				"lastRightsUpdateDate",     // rename of 'lastUpdateDate' and now an int giving date info in the form e.g. 20130810
				"pubDate"				    // changes from string to int
		};
		
		metadata_single_uri = new String[] {
				"schemaVersion",		    /* retained but string ("1.3") now full URI */
				"typeOfResource",			/* retained but string ("text") now full URI (http://id.loc.gov/ontologies/bibframe/Text) */
		};
		
		metadata_single_id_name_type = new String[] {
				"publisher",				/* previously 'imprint', but now LOD triple*/
				"pubPlace",					/* retains name, but now LOD triple */
		};
		
		metadata_multiple = new String[] {
				//"oclc", /* now a single value */
				//"isbn",
				//"issn",
				//"lccn",
/*URI*/			"genre", 	    			// retained, but now URIs
				"language",					// used to be single value in EF1.5, but now in EF2 can be multiple
				"oclc",						// unchanged
				//"names" /* now 'contributor' in LOD */
		};
		
	}
	
	protected void setSingleValueMetadataForFaceting(boolean is_page_level, JSONObject solr_doc_json, String metaname, String metavalue) 
	{	
		if (is_page_level) {
			// In EF1.5 this used to be _htrcstring, but now prefer _htrcstrings to simplify Solr Query syntax for field searching
			solr_doc_json.put("volume"+metaname+"_htrcstrings",metavalue); 		
		}
		else {
			// In EF1.5 this used to be _s, but now prefer _ss to simplify Solr Query syntax for field searching
			solr_doc_json.put(metaname+"_ss",metavalue);
		}
	}
	
	protected void setSingleValueStringMetadata(boolean is_page_level, JSONObject solr_doc_json, String metaname, String metavalue) 
	{
		if (is_page_level) {
			solr_doc_json.put("volume"+metaname+"_txt",metavalue); // tokenized but not stored
			//solr_doc_json.put("volume"+metaname+"_htrcstrings",metavalue); // In EF1.5 this used to be _s, but now prefer _ss to simplify field searching
		}
		else {
			// Want volume-level metadata stored, so can be retrieve in Solr-EF search interface 
			// => '_t' 
			solr_doc_json.put(metaname+"_t",metavalue); // tokenized and stored
			//solr_doc_json.put(metaname+"_ss",metavalue); // In EF1.5 this used to be _s, but now prefer _ss to simplify field searching
		}
		
		setSingleValueMetadataForFaceting(is_page_level, solr_doc_json, metaname, metavalue);
	}
	
	protected void setSingleValueIntegerMetadata(boolean is_page_level, JSONObject solr_doc_json, String metaname, int metavalue_int) 
	{
		if (is_page_level) {
			solr_doc_json.put("volume"+metaname+"_i",metavalue_int); 
		}
		else {
			solr_doc_json.put(metaname+"_i",metavalue_int);
		}
		
		String metavalue_str = Integer.toString(metavalue_int);
		setSingleValueMetadataForFaceting(is_page_level, solr_doc_json, metaname, metavalue_str);	
	}
	
	protected void setSingleValueURIMetadata(boolean is_page_level, JSONObject solr_doc_json, String metaname, String metavalue_uri) 
	{
		// Don't want the URI tokenized in any way, so the _ss or _htrcstrings fields used for faceting
		// are also what we want for regular searching
		setSingleValueMetadataForFaceting(is_page_level, solr_doc_json, metaname, metavalue_uri);	
	}
	
	
	protected void setMultipleValueMetadata(boolean is_page_level, JSONObject solr_doc_json, String metaname, JSONArray metavalues) {
		// Short-cut => can dump the retrieved JSONArray directly into the SolrDoc JSONObject 
		if (is_page_level) {
			solr_doc_json.put("volume"+metaname+"_txt",metavalues);
			solr_doc_json.put("volume"+metaname+"_htrcstrings",metavalues);
		}
		else {
			solr_doc_json.put(metaname+"_t",metavalues);
			solr_doc_json.put(metaname+"_ss",metavalues);
		}
	}
	
	
	protected JSONObject generateMetadataSolrDocJSON(String id, JSONObject ef_metadata, boolean is_page_level)
	{
		/*
		 Example JSON for id: coo.31924013523232
		  "metadata": {
		        "accessProfile": "google",
		        "accessRights": "pd",
		        "contributor": {
		            "id": "http://www.viaf.org/viaf/66826682",
		            "name": "Braddon, M. E. (Mary Elizabeth), 1835-1915.",
		            "type": "http://id.loc.gov/ontologies/bibframe/Person"
		        },
		        "dateCreated": 20200209,
		        "enumerationChronology": "v.1",
		        "genre": "http://id.loc.gov/vocabulary/marcgt/doc",
		        "id": "http://hdl.handle.net/2027/coo.31924013523232",
		        "language": "eng",
		        "lastRightsUpdateDate": 20190603,
		        "mainEntityOfPage": [
		            "https://catalog.hathitrust.org/Record/008668964",
		            "http://catalog.hathitrust.org/api/volumes/brief/oclc/37262723.json",
		            "http://catalog.hathitrust.org/api/volumes/full/oclc/37262723.json"
		        ],
		        "oclc": "37262723",
		        "pubDate": 1873,
		        "pubPlace": {
		            "id": "http://id.loc.gov/vocabulary/countries/gw",
		            "name": "Germany",
		            "type": "http://id.loc.gov/ontologies/bibframe/Place"
		        },
		        "publisher": {
		            "id": "http://catalogdata.library.illinois.edu/lod/entities/ProvisionActivityAgent/ht/B.%20Tauchnitz",
		            "name": "B. Tauchnitz",
		            "type": "http://id.loc.gov/ontologies/bibframe/Organization"
		        },
		        "schemaVersion": "https://schemas.hathitrust.org/EF_Schema_MetadataSubSchema_v_3.0",
		        "sourceInstitution": {
		            "name": "COO",
		            "type": "http://id.loc.gov/ontologies/bibframe/Organization"
		        },
		        "title": "Strangers and pilgrims; a novel,",
		        "type": [
		            "DataFeedItem",
		            "Book"
		        ],
		        "typeOfResource": "http://id.loc.gov/ontologies/bibframe/Text"
		    }
		 
		 */
		

		// For JSON Solr format see:
		//   https://cwiki.apache.org/confluence/display/solr/Uploading+Data+with+Index+Handlers

		//String title= ef_metadata.getString("title");

		JSONObject solr_doc_json = new JSONObject();
		solr_doc_json.put("id", id);

		for (String metaname: metadata_single_string) {
			//String metavalue = null;
			//if (metaname.equals("dateCreated") || metaname.equals("pubDate")) {
			//	metavalue = Integer.toString(ef_metadata.getInt(metaname));
			//}
			//else {
			//metavalue = ef_metadata.getString(metaname);
			//}
			
			Object metavalue_var = ef_metadata.opt(metaname);
			if (metavalue_var != null) {
				//String metavalue = ef_metadata.getString(metaname);
				try {
					String metavalue_str = (String)metavalue_var;

					setSingleValueStringMetadata(is_page_level, solr_doc_json, metaname, metavalue_str);
				}
				catch (java.lang.ClassCastException e) {
					System.err.println("Error when processing id '"+id+"': Failed to cast JSON metadata field '"+metaname+"' to String");
					e.printStackTrace();
				}
			}
		}
		
		for (String metaname: metadata_single_int) {
			Object metavalue_var = ef_metadata.opt(metaname);
			if (metavalue_var != null) {
				int metavalue_int = (int)metavalue_var;
				setSingleValueIntegerMetadata(is_page_level, solr_doc_json, metaname, metavalue_int);
			}
		}
		
		for (String metaname: metadata_single_uri) {
			Object metavalue_var = ef_metadata.opt(metaname);
			if (metavalue_var != null) {
				String metavalue_uri = (String)metavalue_var;
				setSingleValueURIMetadata(is_page_level, solr_doc_json, metaname, metavalue_uri);
			}
		}
		

		for (String metaname: metadata_multiple) {
			
			// Can't do the following:
			//  JSONArray metavalues = ef_metadata.getJSONArray(metaname);
			// as the metaname could be an array or string
			// => Take a more step-wise approach
			
			Object metavalues_var = ef_metadata.opt(metaname); // returns null if does not exist
			if (metavalues_var != null) {
				
				if (metavalues_var instanceof JSONArray) {
					JSONArray metavalues = (JSONArray)metavalues_var;
					//setMultipleValueMetadata(is_page_level, solr_doc_json, metaname, metavalues);
				}
				else if (metavalues_var instanceof String) {
					// Single value case in string format
					String metavalue = (String)metavalues_var;
					//setSingleValueStringMetadata(is_page_level, solr_doc_json, metaname, metavalue);
				}
				else {
					// Unrecognized JSON type for field 'metaname'
					System.err.println("SolrDocJSON2p1::generateMetadataSolrDocJSON(): For document id '"+id+"'"
										+" Expecting JSONArray or String for value of metadata '"+metaname+"'"
										+" but encountered type '"+metavalues_var.getClass() + "'");
					
				}
			}
		}

		// Special cases
		// "type",						/* rename of 'bibliographicFormat' and "" to e.g. [DataFeedItem, Book] */
		
		
		/*array*/		//"sourceInstitution",      /* changes from string to [name,type]
		
		
		return solr_doc_json;

	}	
			
	public ArrayList<String> generateTokenPosCountLangLabels(String volume_id, String page_id, JSONObject ef_page) 
	{
		System.err.println("**** SolrDocJSONEF2p0::generateTokenPosCountLangLabels() has been recoded for singular 'calculatedLanguage' field in EF2.0 but never tested!!!!");
		
		ArrayList<String> lang_list = new ArrayList<String>();

		if (ef_page != null) {
			//String ef_language = ef_page.getString("calculatedLanguage");

			Object ef_language_var = ef_page.opt("calculatedLanguage");
			if (ef_language_var != null) {
				// Consider checking 'var' type first before type-casting if concerned 
				// that JSON EF not guaranteed to be a String
				String ef_language = (String)ef_language_var; 
				lang_list.add(ef_language);
			}
			else {
				System.err.println("Warning: empty calculatedLanguage field for '" + page_id + "'");
			}

		}
		else {
			System.err.println("Warning: null page for '" + page_id + "'");
		}

		return lang_list;
	}
	

	    
	protected void addSolrLanguageTextFields(JSONObject ef_page, ArrayList<POSString> text_al,
													UniversalPOSLangMap universal_langmap,
												    JSONObject solr_doc_json)
	{
		// TODO: remove
	    //System.err.println("**** SolrDocJSONEF2p0::addSolrLanguageTextFields() this needs to change!!!");
		
		//String ef_language = ef_page.getString("calculatedLanguage");
		
		Object ef_language_var = ef_page.opt("calculatedLanguage");
		if (ef_language_var != null) {
			// Consider checking 'var' type first before type-casting if concerned 
			// that JSON EF not guaranteed to be a String
			try {
				String ef_language = (String) ef_language_var; 

				String [] lang_list = new String[] { ef_language };

				int text_len = text_al.size();

				// TODO
				// From here onwards, same as OpenNLP version
				// => Refactor!!

				// Used to separate POS languages from non-POS ones (in code below) 
				// and also index on all EF languages present
				//
				// The code now processes POS and non-POS together, and only picks
				// the language with the highest confidence rating
				//
				// Through the subroutines called, the disastrous (!) decision to
				// apply the English POS model to any language that didn't have its 
				// own POS model is undone.  Only languages with a valid model got
				// Lang+POS fields in the Solr index; the other one a single Lang fields
				// 

				// Deal with POS languages and non-POS at the same time

				HashMap<String,JSONArray> pos_lang_text_field_map = new HashMap<String,JSONArray>();

				for (int ti=0; ti<text_len; ti++) {
					POSString pos_text_value = text_al.get(ti);
					String text_value = pos_text_value.getString();

					String[] pos_tags = pos_text_value.getPOSTags();
					int pos_tags_len = pos_tags.length;

					for (int pti=0; pti<pos_tags_len; pti++) {
						String stanfordnlp_pos_key = pos_tags[pti];

						Tuple2<String,String> lang_pos_pair = universal_langmap.getUniversalLanguagePOSPair(lang_list, stanfordnlp_pos_key);
						String selected_lang = lang_pos_pair._1;
						String upos = lang_pos_pair._2;

						if (upos != null) {
							// POS-tagged language
							String pos_lang_text_field = selected_lang + "_" + upos + "_htrctokentext";
							addToSolrLanguageTextFieldMap(pos_lang_text_field_map,pos_lang_text_field,text_value);
						}

						// Even for a POS language we want a non-POS version so we can perform faster searching
						// when all parts-of-speech are selected by avoiding the need to Boolean AND all the POS terms
						String non_pos_lang_text_field = selected_lang+  "_htrctokentext";
						addToSolrLanguageTextFieldMap(pos_lang_text_field_map,non_pos_lang_text_field,text_value);

						// On top of this, also store text under "alllangs_htrctokentext" field to allow faster 
						// searching when all POS + all languages is selected
						String alllangs_text_field = "alllangs_htrctokentext";
						addToSolrLanguageTextFieldMap(pos_lang_text_field_map,alllangs_text_field,text_value);
					}
				}

				// Now add each of the POS language fields into solr_doc_json
				Set<String> pos_lang_field_keys = pos_lang_text_field_map.keySet();
				for (String plf_key : pos_lang_field_keys) {
					String lang_text_field = plf_key;
					JSONArray json_values = pos_lang_text_field_map.get(plf_key);

					solr_doc_json.put(lang_text_field, json_values); 
				}
			}
			catch (java.lang.ClassCastException e) {
				String id = solr_doc_json.getString("id");
				
				System.err.println("Error when processing id '"+id+"': Failed to cast JSON metadata field 'calculatedLanguage' to String");
				e.printStackTrace();
			}
					
		}
	}
    
}
