package org.hathitrust.extractedfeatures;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
    
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.DoubleAccumulator;
import org.json.JSONArray;
import org.json.JSONObject;

public class PerVolumeUtil implements Serializable
{
	private static final long serialVersionUID = 1L;
	protected String _input_dir;
	protected String _whitelist_filename;
	protected String _langmap_directory;
	
	protected final ArrayList<String> _solr_endpoints;
	protected final int _solr_endpoints_len;
	
	protected String _output_dir;
	
	protected int    _verbosity;
	
	protected WhitelistBloomFilter _whitelist_bloomfilter;
	protected UniversalPOSLangMap _universal_langmap;

	boolean _icu_tokenize;
	boolean _strict_file_io;

	public PerVolumeUtil(String input_dir, String whitelist_filename, String langmap_directory,
				         ArrayList<String> solr_endpoints, String output_dir, int verbosity, 
					     boolean icu_tokenize, boolean strict_file_io)
	{
		System.out.println("*** PerVolumeUtil Constructor, langmap_directory = " + langmap_directory);
		
		_input_dir  = input_dir;
		_whitelist_filename = whitelist_filename;
		_langmap_directory = langmap_directory;
		
		_solr_endpoints = solr_endpoints;
		_solr_endpoints_len = solr_endpoints.size();
		
		//_solr_url   = solr_url;
		_output_dir = output_dir;
		_verbosity  = verbosity;
		
		_icu_tokenize   = icu_tokenize;
		_strict_file_io = strict_file_io;
		
		_whitelist_bloomfilter = null;
		_universal_langmap = null;
	}
	
	public String getInputDir()
	{
		return _input_dir;
	}
	
	public Integer call(Text json_text) throws IOException

	{ 
	        if (_whitelist_filename != null) {

		    synchronized (_whitelist_filename) {
			if (_whitelist_bloomfilter == null) {
			    
			    _whitelist_bloomfilter = new WhitelistBloomFilter(_whitelist_filename,true);
			}
		    }
		}
		
		if (_langmap_directory != null) {

		    synchronized (_langmap_directory) {
			if (_universal_langmap == null) {
			    _universal_langmap = new UniversalPOSLangMap(_langmap_directory);
			}
		    }
		}

		int ef_num_pages = 0;

		String solr_url = null;
		if (_solr_endpoints_len > 0) {
			int random_choice = (int)(_solr_endpoints_len * Math.random());
			solr_url = _solr_endpoints.get(random_choice);
		}
		
		try {


			JSONObject extracted_feature_record  = new JSONObject(json_text.toString());

			if (extracted_feature_record != null) {
				String volume_id = extracted_feature_record.getString("id");

				JSONObject ef_metadata = extracted_feature_record.getJSONObject("metadata");
				//String title= ef_metadata.getString("title");

				//
				// Top-level metadata Solr doc
				//
				JSONObject solr_add_metadata_doc_json = SolrDocJSON.generateToplevelMetadataSolrDocJSON(volume_id,ef_metadata);
				if (solr_add_metadata_doc_json != null) {
				
					if ((_verbosity >=2)) {
						System.out.println("==================");
						System.out.println("Metadata JSON: " + solr_add_metadata_doc_json.toString());
						System.out.println("==================");
					}

					if (solr_url != null) {

						if ((_verbosity >=2) ) {
							System.out.println("==================");
							System.out.println("Posting to: " + solr_url);
							System.out.println("==================");
						}
						SolrDocJSON.postSolrDoc(solr_url, solr_add_metadata_doc_json, volume_id, "top-level-metadata");
					}
				}
				
				//
				// Now move on to POS extracted features per-page
				//
				boolean index_pages = true;
				if (index_pages) {
					
					JSONObject ef_features = extracted_feature_record.getJSONObject("features");

					int ef_page_count = ef_features.getInt("pageCount");

					if (_verbosity >= 1) {
						System.out.println("Processing: " + volume_id);
						System.out.println("  pageCount = " + ef_page_count);
					}

					JSONArray ef_pages = ef_features.getJSONArray("pages");
					ef_num_pages = ef_pages.length();


					for (int i = 0; i < ef_page_count; i++) {
						String formatted_i = String.format("page-%06d", i);
						String page_id = volume_id + "." + formatted_i;

						if (_verbosity >= 2) {
							System.out.println("  Page: " + page_id);
						}


						JSONObject ef_page = ef_pages.getJSONObject(i);

						if (ef_page != null) {
							// Convert to Solr add form
							JSONObject solr_add_doc_json 
							= SolrDocJSON.generateSolrDocJSON(volume_id, page_id, 
												ef_metadata, ef_page,
												_whitelist_bloomfilter, _universal_langmap, _icu_tokenize);


							if ((_verbosity >=2) && (i==20)) {
								System.out.println("==================");
								System.out.println("Sample output Solr add JSON [page 20]: " + solr_add_doc_json.toString());
								System.out.println("==================");
							}


							if (solr_url != null) {
								SolrDocJSON.postSolrDoc(solr_url, solr_add_doc_json,
										volume_id, page_id);
							}
						}
						else {
							System.err.println("Skipping: " + page_id);
						}

					}
				}
				else {
				    System.err.println("Skipping per-page POS text indexing");
				}

			}
		}
		catch (Exception e) {
			if (_strict_file_io) {
				throw e;
			}
			else {
				e.printStackTrace();
			}
		}
		
		return ef_num_pages;

	}
		
	public Integer callAddConcepts(JSONObject vol_rec)
	{
	
		int num_processed = 1;
		
		String solr_url = null;
		if (_solr_endpoints_len > 0) {
			int random_choice = (int)(_solr_endpoints_len * Math.random());
			solr_url = _solr_endpoints.get(random_choice);
		}
		
		String volume_id = vol_rec.getString("volId");
		JSONArray capisco_pages = vol_rec.getJSONArray("pages");
		
		if (capisco_pages != null) {
			

			JSONObject solr_update_concept_metadata_doc_json = SolrDocJSON.generateIncrementalUpdateMetadata(volume_id,capisco_pages);
			if (solr_update_concept_metadata_doc_json != null) {

				if ((_verbosity >=2)) {
					System.out.println("==================");
					System.out.println("Concept JSON: " + solr_update_concept_metadata_doc_json.toString());
					System.out.println("==================");
				}

				if (solr_url != null) {

					if ((_verbosity >=2) ) {
						System.out.println("==================");
						System.out.println("Posting to: " + solr_url);
						System.out.println("==================");
					}
					SolrDocJSON.postSolrDoc(solr_url, solr_update_concept_metadata_doc_json, volume_id, "top-level-metadata");
				}
			}
			else {
				if ((_verbosity >=1)) {
					System.out.println("==================");
					System.out.println("No page-tagged concepts present for: " + volume_id);
					System.out.println("==================");
				}
			}
		}
		
		// now do as page-level ??
		
		
		return num_processed;
	}
		/*
	//public void call(String json_file_in) throws IOException
	public Integer call(String json_file_in) throws IOException
	
	{ 
		if ((_whitelist_filename != null) && (_whitelist_bloomfilter == null)) {
			_whitelist_bloomfilter = new WhitelistBloomFilter(_whitelist_filename,true);
		}

		int ef_num_pages = 0;
		
		ArrayList<String> ids = new ArrayList<String>(); // want it to be non-null so can return valid iterator
		
		String full_json_file_in = _input_dir + "/" + json_file_in;
		JSONObject extracted_feature_record = JSONClusterFileIO.readJSONFile(full_json_file_in);
		
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
			ef_num_pages = ef_pages.length();

			// Make directory for page-level JSON output
			String json_dir = ClusterFileIO.removeSuffix(json_file_in,".json.bz2");
			String page_json_dir = json_dir + "/pages";

			if (_output_dir != null) {
				ClusterFileIO.createDirectoryAll(_output_dir + "/" + page_json_dir);
			}
			
			ids = new ArrayList<String>(ef_num_pages);
			for (int i = 0; i < ef_page_count; i++) {
				String formatted_i = String.format("page-%06d", i);
				String page_id = volume_id + "." + formatted_i;

				if (_verbosity >= 2) {
					System.out.println("  Page: " + page_id);
				}

				String output_json_bz2 = page_json_dir +"/" + formatted_i + ".json.bz2";
				ids.add(page_id); 

				if (_verbosity >=2) {
					if (i==0) {
						System.out.println("Sample output JSON page file [i=0]: " + output_json_bz2);
					}
				}
				JSONObject ef_page = ef_pages.getJSONObject(i);

				if (ef_page != null) {
					// Convert to Solr add form
					JSONObject solr_add_doc_json 
					= SolrDocJSON.generateSolrDocJSON(volume_id, page_id, ef_page, _whitelist_bloomfilter, _icu_tokenize);


					if ((_verbosity >=2) && (i==20)) {
						System.out.println("==================");
						System.out.println("Sample output Solr add JSON [page 20]: " + solr_add_doc_json.toString());
						System.out.println("==================");
					}


					if (_solr_url != null) {
						if ((_verbosity >=2) && (i==20)) {
							System.out.println("==================");
							System.out.println("Posting to: " + _solr_url);
							System.out.println("==================");
						}
						SolrDocJSON.postSolrDoc(_solr_url, solr_add_doc_json);
					}

					if (_output_dir != null) {
						if ((_verbosity >=2) && (i==20)) {
							System.out.println("==================");
							System.out.println("Saving to: " + _output_dir);
							System.out.println("==================");
						}
						SolrDocJSON.saveSolrDoc(solr_add_doc_json, _output_dir + "/" + output_json_bz2);
					}
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
		
		return ef_num_pages;
				
	}
	*/
}

