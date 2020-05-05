package org.hathitrust.extractedfeatures;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.DoubleAccumulator;
import org.json.JSONArray;
import org.json.JSONObject;

class PerVolumeWordStreamFlatmap implements FlatMapFunction<Text, String>
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
	
	public Iterator<String> call(Text json_text) throws IOException
	{ 
	     
		//String full_json_file_in = _input_dir + "/" + json_file_in;
		//JSONObject extracted_feature_record = JSONClusterFileIO.readJSONFile(full_json_file_in);
		
		ArrayList<String> all_word_list = new ArrayList<String>();
		
		String json_text_str = json_text.toString();

		try {
			
			JSONObject extracted_feature_record  = new JSONObject(json_text_str);

			if (extracted_feature_record != null) {
				// EF1.0 and EF1.5 ids where of the form:
				//   hvd.32044090308966
				// EF2.0 now full URI of the form:
				//   https://data.analytics.hathitrust.org/extracted-features/20200210/hvd.32044090308966
				
				String volume_id = extracted_feature_record.getString("id");
				volume_id = volume_id.replaceFirst("https://data.analytics.hathitrust.org/extracted-features/\\d+/", "");
				
				//JSONObject ef_metadata = extracted_feature_record.optJSONObject("metadata");		

				JSONObject ef_features = extracted_feature_record.getJSONObject("features");

				int ef_page_count = ef_features.getInt("pageCount");

				if (_verbosity >= 1) {
					System.out.println("Processing volume id : " + volume_id);
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
						if (page_word_list != null) {
							all_word_list.addAll(page_word_list);
						}
					}
					else {
						System.err.println("Skipping: " + page_id);
					}
				}
			}
		}		
		catch (Exception e) {
			String first_320_chars = json_text_str.substring(0,320);
			
			String mess = "Failed to parse JSON content.  First 320 chars are: '" + first_320_chars + "'";
			
			if (_strict_file_io) {
				//throw e;
				throw new IOException(mess);
			}
			else {
				System.err.println("Warning: " + mess);
				System.out.println("Warning: " + mess);
				
				e.printStackTrace();
			}
		}	

		_progress_accum.add(_progress_step);
		
		return all_word_list.iterator();
	}
	
	
}

