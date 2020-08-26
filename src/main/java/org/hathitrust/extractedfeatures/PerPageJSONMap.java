package org.hathitrust.extractedfeatures;

import java.util.ArrayList;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.json.JSONObject;


class PerPageJSONMap implements Function<JSONObject, String> 
{
	private static final long serialVersionUID = 1L;
	
	protected String _input_dir;
	protected String _output_dir;
	protected int    _verbosity;
	
	protected final ArrayList<String> _solr_endpoints;
	protected final int _solr_endpoints_len;
	
	protected LongAccumulator _progress_accum;
	protected long            _progress_step;
	
	public PerPageJSONMap(String input_dir, ArrayList<String> solr_endpoints, String output_dir, int verbosity, 
					      LongAccumulator progress_accum, long progress_step)
	{
		_input_dir  = input_dir;
		_output_dir = output_dir;
		_verbosity  = verbosity;
		
		_solr_endpoints   = solr_endpoints;
		_solr_endpoints_len = _solr_endpoints.size();
		
		_progress_accum = progress_accum;
		_progress_step  = progress_step;
		
	}
	
	public String call(JSONObject solr_add_doc_json) 
	{ 
		String output_json_bz2 = solr_add_doc_json.getString("filename_json_bz2");
		solr_add_doc_json.remove("filename_json_bz2");
		
		boolean random_test = (Math.random()>0.999); // every 1000
		
		if ((_verbosity >=2) && (random_test)) {
			System.out.println("==================");
			System.out.println("Sample output Solr add JSON [random test 1/1000]: " + solr_add_doc_json.toString());
			System.out.println("==================");
		}
		
		ArrayList<String> solr_url_alts = SolrDocJSON.generateRandomRetrySolrEnpoints(_solr_endpoints);
		/*
		String solr_url = null;
		if (_solr_endpoints_len > 0) {
			int random_choice = (int)(_solr_endpoints_len * Math.random());
			solr_url = _solr_endpoints.get(random_choice);
		}*/
				
		if (solr_url_alts != null) {
			if ((_verbosity >=2) && (random_test)) {
				System.out.println("==================");
				System.out.println("Posting to: " + solr_url_alts.get(0));
				System.out.println("==================");
			}
			SolrDocJSON.postSolrDoc(solr_url_alts, solr_add_doc_json,output_json_bz2,""); // Compromise over debug output
		}

		if (_output_dir != null) {
			if ((_verbosity >=2) && (random_test)) {
				System.out.println("==================");
				System.out.println("Saving to: " + _output_dir);
				System.out.println("==================");
			}
			SolrDocJSON.saveSolrDoc(solr_add_doc_json, _output_dir + "/" + output_json_bz2);
		}
		
		_progress_accum.add(_progress_step);
		
		return output_json_bz2;
	}
	
}

