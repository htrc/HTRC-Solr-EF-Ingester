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
	    ~htBibUrl: "http://catalog.hathitrust.org/api/volumes/full/htid/coo.31924013523232.json" -> now in 'mainEntityOfPage' [3]
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
		~names: [ *** replaced by 'contributor' RDF {id, name, type} of type URI Person + VIAF ID
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
	protected String[] metadata_multiple_uri = null;
	protected String[] metadata_multiple_id_name_type = null;
	
	public SolrDocJSONEF2p0()
	{
		
		metadata_single_string = new String[] {
				"accessProfile",
				"accessRights", 	        // rename of 'rightsAttribute'
				//"handleUrl",			    /* now 'id' but note reason for suppression below*/ 
				//"hathitrustRecordNumber", /* now appears in 'mainEntityOfPage', URI-field [0] and leading 00 padding */
				//"htBibUrl",				/* now in 'mainEntityOfPage' URI-field [2] */
				//"issuance",			    /* removed in EF2*/
				"id",    	            	/* rename of handleUrl, needs special code to avoid clash with 'id' that Solr uses */
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
				"typeOfResource"			/* retained but string ("text") now full URI (http://id.loc.gov/ontologies/bibframe/Text) */
		};
		

		metadata_multiple = new String[] {
				//"isbn", 					/* now gone */
				//"issn", 					/* now gone */
				//"lccn", 					/* now gone */
				"language",					/* used to be single value in EF1.5, but now in EF2 can be multiple */
				"oclc"						/* unchanged */
				//"names" 					/* now 'contributor' in LOD form */
		};
		
		metadata_multiple_uri = new String[] {
				"genre" 	    			/* retained, but now URIs */
		};
		
		metadata_multiple_id_name_type = new String[] {
				"contributor",				/* rename of 'names' and now in LOD form */
				"publisher",				/* previously 'imprint', but now LOD triple*/
				"pubPlace"					/* retains name, but now LOD triple */
		};
	}
	
	protected void setSingleValueMetadataForFaceting(boolean is_page_level, JSONObject solr_doc_json, String metaname, String metavalue) 
	{	
		if (is_page_level) {
			// In EF1.5 this used to be _htrcstring, but now prefer _htrcstrings to simplify Solr Query syntax for field searching
			//solr_doc_json.put("volume"+metaname+"_htrcstrings",metavalue); 
			
			solr_doc_json.put("volume"+metaname+"_htrcstring",metavalue); 		
		}
		else {
			// In EF1.5 this used to be _s, but now prefer _ss to simplify Solr Query syntax for field searching
			//solr_doc_json.put(metaname+"_ss",metavalue);
			
			solr_doc_json.put(metaname+"_s",metavalue);
		}
	}
	
	protected void setSingleValueStringMetadata(boolean is_page_level, JSONObject solr_doc_json, String metaname, String metavalue) 
	{
		if (is_page_level) {
			solr_doc_json.put("volume"+metaname+"_txt",metavalue); // tokenized but not stored
		}
		else {
			// Want volume-level metadata stored, so can be retrieve in Solr-EF search interface 
			// => '_t' 
			solr_doc_json.put(metaname+"_t",metavalue); // tokenized and stored
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
		setSingleValueStringMetadata(is_page_level, solr_doc_json, metaname, metavalue_str); // put in as an indexed text field for good measure
		setSingleValueMetadataForFaceting(is_page_level, solr_doc_json, metaname, metavalue_str);	
	}
	
	protected void setSingleValueURIMetadata(boolean is_page_level, JSONObject solr_doc_json, String metaname, String metavalue_uri) 
	{
		// Don't want the URI tokenized in any way, so use _s or _htrcstring fields
		// This can be accomplished by using the ForFaceting method
		setSingleValueMetadataForFaceting(is_page_level, solr_doc_json, metaname, metavalue_uri);	
	}
	
	
	protected void setMultipleValueMetadataForFaceting(boolean is_page_level, JSONObject solr_doc_json, String metaname, JSONArray metavalues) 
	{
		// Short-cut => can dump the retrieved JSONArray directly into the SolrDoc JSONObject 
		if (is_page_level) {
			solr_doc_json.put("volume"+metaname+"_htrcstrings",metavalues);
		}
		else {
			solr_doc_json.put(metaname+"_ss",metavalues);
		}
	}
	
	protected void setMultipleValueStringMetadata(boolean is_page_level, JSONObject solr_doc_json, String metaname, JSONArray metavalues) 
	{
		// Short-cut => can dump the retrieved JSONArray directly into the SolrDoc JSONObject 
		if (is_page_level) {
			solr_doc_json.put("volume"+metaname+"_txt",metavalues); // not stored
		}
		else {
			solr_doc_json.put(metaname+"_t",metavalues); // stored
		}
		setMultipleValueMetadataForFaceting(is_page_level, solr_doc_json, metaname, metavalues);
	}
	
	protected void setMultipleValueURIMetadata(boolean is_page_level, JSONObject solr_doc_json, String metaname, JSONArray metavalues) 
	{
		JSONArray filtered_metavalues = new JSONArray();
		for (int i=0; i<metavalues.length(); i++) {
			String uri = metavalues.getString(i);
			if (uri.startsWith("urn:uuid")) {
				continue;
			}
			filtered_metavalues.put(uri);
		}
		
		if (filtered_metavalues.length() > 0 ) {
			setMultipleValueMetadataForFaceting(is_page_level, solr_doc_json, metaname, filtered_metavalues);
		}
	}
	
	
	protected JSONObject generateMetadataSolrDocJSON(String id, JSONObject ef_metadata, boolean is_page_level)
	{
		/*
		 Example JSON EF2.0 for id: coo.31924013523232
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

		JSONObject solr_doc_json = new JSONObject();
		solr_doc_json.put("id", id); // Required field, as per the defined Solr schema

		for (String metaname: metadata_single_string) {
			
			if (!ef_metadata.isNull(metaname)){
				String metavalue_str = ef_metadata.getString(metaname);
				if (metaname.equals("id")) {
					// Guard against clash with 'id' used by Solr
					metaname = "htid";
				}
				setSingleValueStringMetadata(is_page_level, solr_doc_json, metaname, metavalue_str);
			}
		}
		
		for (String metaname: metadata_single_int) {
			if (!ef_metadata.isNull(metaname)) {
				try {
					int metavalue_int = ef_metadata.getInt(metaname);
					setSingleValueIntegerMetadata(is_page_level, solr_doc_json, metaname, metavalue_int);
				}
				catch (org.json.JSONException e) {
					if (!is_page_level) {
						// To avoid unnecessary spamming of the error, 
						// only print it out for the top-level volume metadata case

						System.err.println("**** Error: For id = '"+id+"' accessing '"+metaname+"' as an int");
						e.printStackTrace();
					}
					
					String metavalue_str = ef_metadata.getString(metaname);
					if (!is_page_level) {
						System.err.println("**** Saving the value '"+metavalue_str+"' as an indexed string");
					}
					setSingleValueStringMetadata(is_page_level, solr_doc_json, metaname, metavalue_str);
				}
			}
		}
		
		for (String metaname: metadata_single_uri) {
			if (!ef_metadata.isNull(metaname)) {
				//String metavalue_uri = (String)metavalue_var;
				String metavalue_uri = ef_metadata.getString(metaname);
				setSingleValueURIMetadata(is_page_level, solr_doc_json, metaname, metavalue_uri);
			}
		}
		
		
		for (String metaname: metadata_multiple) {
			
			// Can't do the following:
			//  JSONArray metavalues = ef_metadata.getJSONArray(metaname);
			// as the metaname could be an array or string
			// => Need to take a more step-wise approach
			
			if (!ef_metadata.isNull(metaname)) {
				Object metavalues_var = ef_metadata.get(metaname); 
	
				if (metavalues_var instanceof JSONArray) {
					JSONArray metavalues = (JSONArray)metavalues_var;
					setMultipleValueStringMetadata(is_page_level, solr_doc_json, metaname, metavalues);
				}
				else if (metavalues_var instanceof String) {
					// Single value case in string format
					String metavalue = (String)metavalues_var;
					
					// Wrap it up as an array, so treated consistently with other multiple value entries
					JSONArray metavalues = new JSONArray();
					metavalues.put(metavalue);
					
					setMultipleValueStringMetadata(is_page_level, solr_doc_json, metaname, metavalues);
				}
				else {
					// Unrecognized JSON type for field 'metaname'
					if (!is_page_level) {
						// To avoid unnecessary spamming of the error, 
						// only print it out for the top-level volume metadata case
						System.err.println("SolrDocJSON2p0::generateMetadataSolrDocJSON(): For document id '"+id+"'"
								+" Expecting JSONArray or String for value of metadata '"+metaname+"'"
								+" but encountered type '"+metavalues_var.getClass() + "'");	
					}
				}
			}
		}

		for (String metaname: metadata_multiple_uri) {
			
			if (!ef_metadata.isNull(metaname)) {
				Object metavalues_var = ef_metadata.get(metaname); 
	
				if (metavalues_var instanceof JSONArray) {
					JSONArray metavalues = (JSONArray)metavalues_var;
					setMultipleValueURIMetadata(is_page_level, solr_doc_json, metaname, metavalues);
				}
				else if (metavalues_var instanceof String) {
					// Single value case in string format
					String metavalue = (String)metavalues_var;
					
					// Wrap it up as an array, so treated consistently with other multiple value entries
					JSONArray metavalues = new JSONArray();
					metavalues.put(metavalue);
					
					setMultipleValueURIMetadata(is_page_level, solr_doc_json, metaname, metavalues);
				}
				else {
					// Unrecognized JSON type for field 'metaname'
					if (!is_page_level) {
						// To avoid unnecessary spamming of the error, 
						// only print it out for the top-level volume metadata case
						System.err.println("SolrDocJSON2p1::generateMetadataSolrDocJSON(): For document id '"+id+"'"
								+" Expecting JSONArray or String for value of metadata '"+metaname+"'"
								+" but encountered type '"+metavalues_var.getClass() + "'");	
					}
				}
			}
		}
		
		for (String metaname: metadata_multiple_id_name_type) {
			if (!ef_metadata.isNull(metaname)) {
				Object metavalues_var = ef_metadata.get(metaname); 
				
				if (metavalues_var instanceof JSONArray) {
					JSONArray metavalues_id_name_type = (JSONArray)metavalues_var;
					JSONArray metavalues_id   = new JSONArray();
					JSONArray metavalues_name = new JSONArray();
					JSONArray metavalues_type = new JSONArray();
					
					int metavalues_len = metavalues_id_name_type.length();
					for (int i=0; i<metavalues_len; i++) {
						
						JSONObject metavalue_id_name_type = metavalues_id_name_type.getJSONObject(i);
						String metavalue_id   = metavalue_id_name_type.getString("id");
						String metavalue_name = metavalue_id_name_type.getString("name");
						String metavalue_type = metavalue_id_name_type.getString("type");

						metavalues_id.put(metavalue_id);
						metavalues_name.put(metavalue_name);
						metavalues_type.put(metavalue_type);
					}
					
					setMultipleValueURIMetadata(is_page_level, solr_doc_json, metaname+"Id",   metavalues_id);
					setMultipleValueStringMetadata(is_page_level, solr_doc_json, metaname+"Name", metavalues_name);
					setMultipleValueURIMetadata(is_page_level, solr_doc_json, metaname+"Type", metavalues_type);
				}
				else {
					// If not an array or {}, then a single {}
					JSONObject metavalue_id_name_type = ef_metadata.getJSONObject(metaname);
					try {
						String metavalue_id = metavalue_id_name_type.getString("id");
						String metavalue_name = metavalue_id_name_type.getString("name");
						String metavalue_type = metavalue_id_name_type.getString("type");

						JSONArray metavalues_id   = new JSONArray();
						JSONArray metavalues_name = new JSONArray();
						JSONArray metavalues_type = new JSONArray();

						metavalues_id.put(metavalue_id);
						metavalues_name.put(metavalue_name);
						metavalues_type.put(metavalue_type);

						setMultipleValueURIMetadata(is_page_level, solr_doc_json, metaname+"Id",   metavalues_id);
						setMultipleValueStringMetadata(is_page_level, solr_doc_json, metaname+"Name", metavalues_name);
						setMultipleValueURIMetadata(is_page_level, solr_doc_json, metaname+"Type", metavalues_type);
					}
					catch (Exception e) {
						if (!is_page_level) {
							// To avoid unnecessary spamming of the error, 
							// only print it out for the top-level volume metadata case

							System.err.println("**** Error: For id = '"+id+"' processing '"+metaname+"' as a JSONObject");
							System.err.println("**** where metavalue was: "+metavalue_id_name_type.toString());
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		// Special cases
		
		// "type"				
		//   Rename of 'bibliographicFormat' and changes from string to array e.g. [DataFeedItem, Book] */
		JSONArray type_metavalue_array= ef_metadata.getJSONArray("type");
		
		String type_metavalue_item = type_metavalue_array.getString(0);
		String type_metavalue_bibformat = type_metavalue_array.getString(1);
		
		if (!type_metavalue_item.equals("DataFeedItem")) {
			System.err.println("**** Warning: For id = '"+id+"' the metadata entry for type[0] was "
					+"'"+type_metavalue_item +"' rather than 'DataFeedItem'");
		}
		else {
			setSingleValueStringMetadata(is_page_level, solr_doc_json, "bibliographicFormat", type_metavalue_bibformat);
		}
		
		// "sourceInstitution"
		//   Retained, but value changes from string to {name, type} 
		//   e.g., [name: "COO", "type": "http://id.loc.gov/ontologies/bibframe/Organization"
		JSONObject sourceinst_metavalue_object= ef_metadata.getJSONObject("sourceInstitution");
		
		String sourceinst_metavalue_name = sourceinst_metavalue_object.getString("name");
		String sourceinst_metavalue_type = sourceinst_metavalue_object.getString("type");
		
		if (!sourceinst_metavalue_type.equals("http://id.loc.gov/ontologies/bibframe/Organization")) {
			System.err.println("**** Warning: For id = '"+id+"' the metadata entry for sourceInstitute.type was "
					+"'"+sourceinst_metavalue_type +"' rather than 'http://id.loc.gov/ontologies/bibframe/Organization'");
		}
		else {
			setSingleValueStringMetadata(is_page_level, solr_doc_json, "sourceInstitution", sourceinst_metavalue_name);
		}
		
		
		// "mainEntityOfPage" 
		//   New field, for example:
		//   mainEntityOfPage: [
		// 		            "https://catalog.hathitrust.org/Record/008668964",
		// 		            "http://catalog.hathitrust.org/api/volumes/brief/oclc/37262723.json",
		// 		            "http://catalog.hathitrust.org/api/volumes/full/oclc/37262723.json"
		// 		        ]
		
		JSONArray meop_metavalue_array = ef_metadata.getJSONArray("mainEntityOfPage");
		
		/*
		if (meop_metavalue_array.length() != 3) {
			if (!is_page_level) {
				// To avoid unnecessary spamming of the error, 
				// only print it out for the top-level volume metadata case
				System.err.println("**** Warning: For id = '"+id+"' the metadata entry for 'mainEntityOfPage' contained "
						+ meop_metavalue_array.length() +" items, when 3 were expected");
				System.err.println("**** Indexing mainEntityOfPage value as is: " + meop_metavalue_array.toString());
			}
		}
	*/
		String meop_first_val = meop_metavalue_array.getString(0);
		
		if (meop_first_val.startsWith("https://catalog.hathitrust.org/Record/") && (!ef_metadata.isNull("oclc"))) {
			setSingleValueURIMetadata(is_page_level, solr_doc_json, "mainEntityOfPageRecord", meop_first_val);
		}
		else {
			if (!is_page_level) {
				// To avoid unnecessary spamming of the error, 
				// only print it out for the top-level volume metadata case
				System.err.println("**** Warning: For id = '"+id+"' the metadata entry for 'mainEntityOfPage' was: "
						+ meop_metavalue_array.toString() + ", and no companion 'oclc' entry was found");
			}
		}
		
		// TODO
		// Consider removing the following, if above mainEntityOfPageRecord doesn't lead to any warning being issued
		setMultipleValueURIMetadata(is_page_level, solr_doc_json, "mainEntityOfPage", meop_metavalue_array);
		/*
		for (int i=0; i<3; i++) {
			try {
			String meop_metavalue = meop_metavalue_array.getString(i);
			setMultipleValueURIMetadata(is_page_level, solr_doc_json, "mainEntityOfPage", meop_metavalue);
			}
			catch (org.json.JSONException e) {
				System.err.println("**** Error: For id = '"+id+"' accessing mainEntityOfPage["+i+"] threw exception");
				e.printStackTrace();
			}
		}*/
			
		
		return solr_doc_json;
	}	
			
	public ArrayList<String> generateTokenPosCountLangLabels(String volume_id, String page_id, JSONObject ef_page) 
	{
		System.err.println("**** SolrDocJSONEF2p0::generateTokenPosCountLangLabels() has been recoded for singular 'calculatedLanguage' field in EF2.0 but never tested!!!!");
		
		ArrayList<String> lang_list = new ArrayList<String>();

		if ((ef_page != null) && (ef_page != JSONObject.NULL)) {

			if (!ef_page.isNull("calculatedLanguage")) {

				String ef_language = ef_page.getString("calculatedLanguage");				
				lang_list.add(ef_language);
			}
			// No need to print out warning if 'calculatedLanguage' is null,
			// as this is deliberately used to represent the case where no language
			// could be identified based on the text present on the page
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

		if (!ef_page.isNull("calculatedLanguage")) {
			// Consider checking 'var' type first before type-casting if concerned 
			// that JSON EF not guaranteed to be a String
			//try {
				String ef_language = ef_page.getString("calculatedLanguage");
				
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
			//}
			//catch (java.lang.ClassCastException e) {
			//	String id = solr_doc_json.getString("id");
			//	
			//	System.err.println("Error when processing id '"+id+"': Failed to cast JSON metadata field 'calculatedLanguage' to String");
			//	e.printStackTrace();
			//}
					
		}
	}
    
}
