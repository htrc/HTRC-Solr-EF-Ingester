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
	
	protected String [] metadata_hashmap_multiple = null;
	
	public SolrDocJSONEF1p5()
	{
		
		metadata_single_string = new String[] {
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

		for (String metaname: metadata_single_string) {
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
			
	public ArrayList<String> generateTokenPosCountLangLabels(String volume_id, String page_id, JSONObject ef_page)
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
	


	    
	protected void addSolrLanguageTextFields(JSONObject ef_page, ArrayList<POSString> text_al,
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
			
			
			/*
			for (int li=0; li<lang_len; li++) {
				String lang_key = lang_list[li];
				
				if (universal_langmap.containsLanguage(lang_key))
				{
				*/
					// Deal with POS languages and non-POS at the same time
			
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
							
							/*
							if (!pos_lang_text_field_map.containsKey(pos_lang_text_field)) {
								JSONArray empty_json_values = new JSONArray();
								pos_lang_text_field_map.put(pos_lang_text_field, empty_json_values);
							}
							pos_lang_text_field_map.get(pos_lang_text_field).put(text_value);*/
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
    
}
