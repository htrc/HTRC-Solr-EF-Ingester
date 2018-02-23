package org.hathitrust.extractedfeatures;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.DoubleAccumulator;
import org.json.JSONArray;
import org.json.JSONObject;

class PerVolumeWordStreamFlatmap implements FlatMapFunction<String, String>
{
	private static final long serialVersionUID = 1L;
	
	protected String _input_dir;
	protected int    _verbosity;
	
	protected DoubleAccumulator _progress_accum;
	protected double            _progress_step;
	
	boolean _icu_tokenize;
	boolean _strict_file_io;
	
	public PerVolumeWordStreamFlatmap(String input_dir, int verbosity, 
					 		  DoubleAccumulator progress_accum, double progress_step,
					 		  boolean icu_tokenize,
					 		  boolean strict_file_io)
	{
		_input_dir  = input_dir;
		_verbosity  = verbosity;
		
		_progress_accum = progress_accum;
		_progress_step  = progress_step;
		
		_icu_tokenize   = icu_tokenize;
		_strict_file_io = strict_file_io;
	}
	
	public Iterator<String> call(String json_file_in) throws IOException
	{ 
	     
		String full_json_file_in = _input_dir + "/" + json_file_in;
		JSONObject extracted_feature_record = JSONClusterFileIO.readJSONFile(full_json_file_in);
		
		ArrayList<String> all_word_list = new ArrayList<String>();
		
		if (extracted_feature_record != null) {
			String volume_id = extracted_feature_record.getString("id");

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
	
			if (_verbosity >= 2) {
				System.out.print("  Pages: ");
			}

			for (int i = 0; i < ef_page_count; i++) {
				String formatted_i = String.format("page-%06d", i);
				String page_id = volume_id + "." + formatted_i;

				if (_verbosity >= 2) {
					if (i>0) {
						System.out.print(", ");
					}
					System.out.print(page_id);
				}

				if (i==(ef_page_count-1)) {
					if (_verbosity >= 2) {
						System.out.println();
					}
				}

				JSONObject ef_page = ef_pages.getJSONObject(i);

				if (ef_page != null) {
					
					ArrayList<String> page_word_list = SolrDocJSON.generateTokenPosCountWhitelistText(volume_id, page_id, ef_page, _icu_tokenize);					
					all_word_list.addAll(page_word_list);
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
		
		return all_word_list.iterator();
	}
	
	
}

