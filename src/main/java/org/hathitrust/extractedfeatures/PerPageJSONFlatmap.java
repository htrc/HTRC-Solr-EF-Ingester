package org.hathitrust.extractedfeatures;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.DoubleAccumulator;
import org.json.JSONArray;
import org.json.JSONObject;

/*
class PagedJSON implements Function<String, Boolean> {

	private static final long serialVersionUID = 1L;

	public Boolean call(String s) { return s.contains("a"); }
}
 */


class PerPageJSONFlatmap implements FlatMapFunction<String, JSONObject>
//public class PagedJSON implements VoidFunction<String> 
{
	private static final long serialVersionUID = 1L;
	
	protected String _input_dir;
	protected String _whitelist_filename;

	protected String _solr_url;
	protected String _output_dir;
	
	protected int    _verbosity;
	
	protected SolrDocJSON _solr_doc_json;
	
	protected WhitelistBloomFilter _whitelist_bloomfilter;
	protected UniversalPOSLangMap _universal_langmap = null;
	
	protected DoubleAccumulator _progress_accum;
	protected double            _progress_step;
	
	boolean _icu_tokenize;
	boolean _strict_file_io;
	
	public PerPageJSONFlatmap(String input_dir, String whitelist_filename, 
							  String solr_url, String output_dir, int verbosity, 
					 		  DoubleAccumulator progress_accum, double progress_step,
					 		  boolean icu_tokenize, boolean strict_file_io)
	{
		_input_dir  = input_dir;
		_whitelist_filename = whitelist_filename;
		
		_solr_url   = solr_url;
		_output_dir = output_dir;
		_verbosity  = verbosity;
		
		_progress_accum = progress_accum;
		_progress_step  = progress_step;
		
		_icu_tokenize   = icu_tokenize;
		_strict_file_io = strict_file_io;
		
		_solr_doc_json = new SolrDocJSONEF1p5(); // EF 1.5
		
		_whitelist_bloomfilter = null;
	}
	
	
	public Iterator<JSONObject> call(String json_file_in) throws IOException
	{ 
		if ((_whitelist_filename != null) && (_whitelist_bloomfilter == null)) {
			_whitelist_bloomfilter = new WhitelistBloomFilter(_whitelist_filename,true);
		}
		
	 	String full_json_file_in = _input_dir + "/" + json_file_in;
		JSONObject extracted_feature_record = JSONClusterFileIO.readJSONFile(full_json_file_in);
	
		ArrayList<JSONObject> json_pages = new ArrayList<JSONObject>();
		
		if (extracted_feature_record != null) {
			String volume_id = extracted_feature_record.getString("id");

			//JSONObject ef_metadata = extracted_feature_record.getJSONObject("metadata");
			//String title= ef_metadata.getString("title");

			JSONObject ef_features = extracted_feature_record.getJSONObject("features");


			int ef_page_count = ef_features.getInt("pageCount");

			if (_verbosity >= 1) {
				System.out.println("Processing: " + json_file_in);
				System.out.println("  pageCount = " + ef_page_count);
			}

			JSONArray ef_pages = ef_features.getJSONArray("pages");
			int ef_num_pages = ef_pages.length();
			if (ef_num_pages != ef_page_count) {
				System.err.println("Warning: number of page elements in JSON (" + ef_num_pages + ")"
						+" does not match 'pageCount' metadata (" + ef_page_count + ")"); 
			}

			// Make directory for page-level JSON output
			String json_dir = ClusterFileIO.removeSuffix(json_file_in,".json.bz2");
			String page_json_dir = json_dir + "/pages";

			if (_output_dir != null) {
				// Only need to do this once per volume, so easier to here than in the per-page Map
				ClusterFileIO.createDirectoryAll(_output_dir + "/" + page_json_dir);
			}
			if (_verbosity >= 3) {
				System.out.print("  Pages: ");
			}

			for (int i = 0; i < ef_page_count; i++) {
				String formatted_i = String.format("page-%06d", i);
				String page_id = volume_id + "." + formatted_i;

				if (_verbosity >= 3) {
					if (i>0) {
						System.out.print(", ");
					}
					System.out.print(page_id);
				}

				String output_json_bz2 = page_json_dir +"/" + formatted_i + ".json.bz2";
				
				if (i==(ef_page_count-1)) {
					if (_verbosity >= 3) {
						System.out.println();
					}
					if (_verbosity >= 2) {
						System.out.println("Sample output JSON page file: " + output_json_bz2);
					}
				}

				JSONObject ef_metadata = extracted_feature_record.getJSONObject("metadata");
				JSONObject ef_page = ef_pages.getJSONObject(i);

				if (ef_page != null) {
					// Convert to Solr add form
					JSONObject solr_add_doc_json 
						= _solr_doc_json.generateSolrDocJSON(volume_id, page_id, ef_metadata, ef_page, 
													_whitelist_bloomfilter, _universal_langmap,
													_icu_tokenize);
					solr_add_doc_json.put("filename_json_bz2", output_json_bz2);

					json_pages.add(solr_add_doc_json);


				}
				else {
					System.err.println("Skipping: " + page_id);
				}

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
		
		return json_pages.iterator();
	}
	
	
}

