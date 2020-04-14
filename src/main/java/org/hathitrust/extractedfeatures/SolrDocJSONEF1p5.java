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

// SolrDocJSONEF1.5 (1 point 5)
public class SolrDocJSONEF1p5 extends SolrDocJSON 
{	
	/* Extracted Features 2.0 */
	/*
{
  features: {
    id: "http://hdl.handle.net/2027/coo.31924013523232" (field wasn't previously present)

   pages: [
     {
      seq: 00000001
      languages [] -> calculatedLanguage  (2 letter code, or null)
      header, body, footer can be null
     }
    ]
  }
  id: -> htid
  id: "https://data.analytics.hathitrust.org/extracted-features/20200210/coo.31924013523232"
  metadata: {
    =accessProfile: remains unchanged
    ~accessRights: "pd" (new name, renamed from rightsAttributes)
    ~bibliographicFormat: "BK" -> now managed as 'type' [ "DataFeedItem", "Book" ]
  ??-classification: {} -> gone??? or it just that my example was empty/null???
    contributor: {} (rdf: id, name, type) -> replaces 'names' [], but now singular???
    =dateCreated: "" remains, but no longer date and time, now only down to the day e.g. 20200209
	=genre: [] -> "" remains, but no longer an array, now single URI
	-governmentDocument: false -> gone
    ~handleUrl -> now 'id' in new format
	~hathitrustRecordNumber": "8668964" -> now appers in 'mainEntityOfPage', URI-ified [0] and leading 00 padding
    ~htBibUrl: "http://catalog.hathitrust.org/api/volumes/full/htid/coo.31924013523232.json" -> now in 'mainEntityOfPage' [2]
	~id: "http://hdl.handle.net/2027/coo.31924013523232" renamed from previous handleUrl
    ~imprint: "B. Tauchnitz, 1873." -> now 'publisher' as RDF triple (id, name, type)
    isbn: [] -> gone or because empty???
    issn: [] -> gone or because empty???
    -issuance: "monographic" -> gone
	lastUpdateDate: "2014-01-02 06:25:28" -> could this now be lastRightsUpdateDate: 20190603 (int)
	lastRightsUpdateDate: 20190603 -> new if not lastUpdateDate
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
	
	public SolrDocJSONEF1p5()
	{
		
		metadata_single = new String[] {
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

		metadata_multiple = new String[] {
				"oclc",
				"isbn",
				"issn",
				"lccn",
				"genre", 	    
				"names"
		};

		metadata_hashmap_multiple = new String[] {
				"classification"
		};
		
	}
	
	protected JSONObject generateMetadataSolrDocJSON(String id, JSONObject ef_metadata, boolean is_page_level)
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
			
	protected JSONObject generateSolrDocJSON(String volume_id, String page_id, 
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

    
}
