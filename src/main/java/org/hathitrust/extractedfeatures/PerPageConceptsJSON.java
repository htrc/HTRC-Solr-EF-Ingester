package org.hathitrust.extractedfeatures;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.DoubleAccumulator;
import org.json.JSONArray;
import org.json.JSONObject;

public class PerPageConceptsJSON implements Function<String,Integer>
{
	private static final long serialVersionUID = 1L;
	
	protected final ArrayList<String> _solr_endpoints;
	protected final int _solr_endpoints_len;
	
	protected int    _verbosity;

	public PerPageConceptsJSON(ArrayList<String> solr_endpoints, int verbosity)					    
	{
		_solr_endpoints = solr_endpoints;
		_solr_endpoints_len = solr_endpoints.size();
		
		_verbosity  = verbosity;
	}
	
	public Integer callAddConceptsPageLevel(JSONObject page_rec)
	{
		ArrayList<String> solr_url_alts = SolrDocJSON.generateRandomRetrySolrEnpoints(_solr_endpoints);
		/*
		String solr_url = null;
		if (_solr_endpoints_len > 0) {
			int random_choice = (int)(_solr_endpoints_len * Math.random());
			solr_url = _solr_endpoints.get(random_choice);
		}
		*/
		
		String volume_id = page_rec.getString("documentId");
		String collection_name = page_rec.getString("collectionName");
		if (!volume_id.equals(collection_name)) {
			volume_id = collection_name+"/"+volume_id;
		}
		String page_id_filename = page_rec.getString("pageId");
		
		Pattern page_id_pattern = Pattern.compile("(\\d{6})\\.txt$"); // Solr operates on 6-zero padded page Ids
		Matcher page_id_matcher = page_id_pattern.matcher(page_id_filename);
		String page_id;
		if (page_id_matcher.find()) {		    
		    page_id = "page-"+page_id_matcher.group(1);
		}
		else {
		    System.err.println("Error: Failed to find valid page-id in: '" + page_id_filename +"'");
		    page_id = null;
		}
		
		JSONArray solr_update_concept_metadata_doc_json = SolrDocJSON.generateIncrementalPageUpdateMetadata(volume_id,page_id,page_rec);
		if (solr_update_concept_metadata_doc_json != null) {

			if ((_verbosity >=2)) {
				System.out.println("==================");
				System.out.println("Concept JSON: " + solr_update_concept_metadata_doc_json.toString());
				System.out.println("==================");
			}

			if (solr_url_alts != null) {

				if ((_verbosity >=2) ) {
					System.out.println("==================");
					System.out.println("Posting to: " + solr_url_alts.get(0));
					System.out.println("==================");
				}
				SolrDocJSON.postSolrDoc(solr_url_alts, solr_update_concept_metadata_doc_json, volume_id, page_id);
				
				// Send explicit commitWithin
				//JSONObject solr_commitwithin_json = SolrDocJSON.explicitCommitWithin();
				//SolrDocJSON.postSolrDoc(solr_url, solr_commitwithin_json, volume_id, page_id);
			}
		}
		else {
			if ((_verbosity >=1)) {
				System.out.println("==================");
				System.out.println("No page-tagged concepts present for: " + page_id);
				System.out.println("==================");
			}
		}
	
		return 1;
	}
	
	public Integer call(String json_concept_page_rec) throws IOException
	{
		JSONObject page_rec = new JSONObject(json_concept_page_rec);
		return callAddConceptsPageLevel(page_rec);
	}
}

