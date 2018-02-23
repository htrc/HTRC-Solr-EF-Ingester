package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.*;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.hathitrust.extractedfeatures.PerPageJSONFlatmap;
import org.json.JSONObject;
import org.apache.spark.SparkConf;

public class ProcessForSolrIngestJSONFilelist implements Serializable
{
	private static final long serialVersionUID = 1L;

	protected static final int DEFAULT_NUM_CORES = 10;
	protected static final int MINIMUM_NUM_PARTITIONS = 10*DEFAULT_NUM_CORES; 
	
	protected static final int DEFAULT_FILES_PER_PARTITION = 3000;
	
	protected String _input_dir;
	protected String _json_list_filename;
	
	protected String _solr_base_url;
	protected String _solr_collection;
	
	protected String _whitelist_filename;
	protected String _langmap_directory;
	
	//protected String _solr_url;
	protected String _output_dir;
	
	protected int    _verbosity;

	public ProcessForSolrIngestJSONFilelist(String input_dir, String json_list_filename,
											String solr_collection, String solr_base_url, 
											String output_dir, int verbosity)
	{
		_input_dir = input_dir;
		_json_list_filename = json_list_filename;
		
		_solr_collection = solr_collection;
		_solr_base_url   = solr_base_url;
		
		boolean use_whitelist = Boolean.getBoolean("wcsa-ef-ingest.use-whitelist");
		_whitelist_filename = (use_whitelist) ?  System.getProperty("wcsa-ef-ingest.whitelist-filename") : null;
		
		boolean use_langmap = Boolean.getBoolean("wcsa-ef-ingest.use-langmap");
		_langmap_directory = (use_langmap) ?  System.getProperty("wcsa-ef-ingest.langmap-directory") : null;
		
		_output_dir = output_dir;
		_verbosity  = verbosity;
	}

	protected String generateSparkAppName(String exec_mode)
	{
		String spark_app_name = "[" + exec_mode + "] Extract Features: Process for Solr Ingest from JSON-filelist";
		spark_app_name += " [" + _solr_collection + "]";

		if (_solr_base_url != null) { 
			spark_app_name += " solr_base_url=" + _solr_base_url;
		}
		
		if (_output_dir != null) { 
			spark_app_name += " output_dir=" + _output_dir;
		}
		
		return spark_app_name;
	}
	
	public ArrayList<String> extrapolateSolrEndpoints(String solr_collection)
	{
		ArrayList<String> solr_endpoints = new ArrayList<String>();
		
		if (_solr_base_url != null) {
			String solr_url = _solr_base_url + "/" + solr_collection + "/update";
			
			String solr_cloud_nodes = System.getProperty("wcsa-ef-ingest.solr-cloud-nodes",null);
			if (solr_cloud_nodes != null) {
				String [] cloud_nodes = solr_cloud_nodes.split(",");
				for (String cn : cloud_nodes) {
					String solr_endpoint = solr_url.replaceFirst("//.*?:\\d+/", "//"+cn+"/");
					solr_endpoints.add(solr_endpoint);
				}
			}
			else {
				solr_endpoints.add(solr_url);
			}
		}
		
		return solr_endpoints;
	}
	
	public void execPerVolumeJSONFilelist()
	{	
		String spark_app_name = generateSparkAppName("Per Volume");		
		
		SparkConf spark_conf = new SparkConf().setAppName(spark_app_name);
		JavaSparkContext jsc = new JavaSparkContext(spark_conf);
		
		//String filename_root = _json_list_filename.replaceAll(".*/","").replaceAll("\\..*$","");
		
		//int num_partitions = Integer.getInteger("wcsa-ef-ingest.num-partitions", DEFAULT_NUM_PARTITIONS);
		int files_per_partition = Integer.getInteger("wcsa-ef-ingest.files-per-partition", DEFAULT_FILES_PER_PARTITION);
		
		JavaRDD<String> json_list_data = jsc.textFile(_json_list_filename).cache();
		json_list_data.setName("JSON-file-list");
		
		long num_volumes = json_list_data.count();
		double per_vol = 100.0/(double)num_volumes;
		
		int num_partitions = (int)(num_volumes/files_per_partition)+1;
		if (num_partitions < MINIMUM_NUM_PARTITIONS) {
			num_partitions = MINIMUM_NUM_PARTITIONS;
		}
		JavaRDD<String> json_list_data_rp = json_list_data.repartition(num_partitions);
		json_list_data_rp.setName("JSON-file-list--repartitioned");
		
		DoubleAccumulator progress_accum = jsc.sc().doubleAccumulator("Per Volume Progress Percent");
		
		boolean icu_tokenize = Boolean.getBoolean("wcsa-ef-ingest.icu-tokenize");
		boolean strict_file_io = Boolean.getBoolean("wcsa-ef-ingest.strict-file-io");
		
		ArrayList<String> solr_endpoints = extrapolateSolrEndpoints(_solr_collection);
		
		PerVolumeJSONList per_vol_json = new PerVolumeJSONList(_input_dir,_whitelist_filename, _langmap_directory,
														solr_endpoints,_output_dir,_verbosity,
														icu_tokenize,strict_file_io);

		JavaRDD<Integer> per_volume_page_count = json_list_data_rp.map(per_vol_json);
		per_volume_page_count.setName("volume-page-counts");
		
		//Integer total_page_count = volume_page_counts.reduce((a, b) -> a + b);
		long num_vol_ids = per_volume_page_count.count();
		
		System.out.println("");
		System.out.println("############");
		System.out.println("# Number of volume ids: " + num_vol_ids);
		System.out.println("############");
		System.out.println("");

		jsc.close();
	}
	
	public static void print_usage(HelpFormatter formatter, Options options)
	{
		formatter.printHelp("RUN.bash [options] input-dir json-filelist.txt solr-collection", options);
	}
	
	public static void main(String[] args) {
		Options options = new Options();

		Option verbosity_opt = new Option("v", "verbosity", true, 
				"Set to control the level of debugging output [0=none, 1=some, 2=lots]");
		verbosity_opt.setRequired(false);
		options.addOption(verbosity_opt);
		
		Option properties_opt = new Option("p", "properties", true, 
				"Read in the specified Java properties file");
		properties_opt.setRequired(false);
		options.addOption(properties_opt);
		
		Option output_dir_opt = new Option("o", "output-dir", true, 
				"If specified, save BZipped Solr JSON files to this directory");
		output_dir_opt.setRequired(false);
		options.addOption(output_dir_opt);
		
		Option solr_base_url_opt = new Option("u", "solr-base-url", true, 
				"If specified, the base URL to post the Solr JSON data to");
		solr_base_url_opt.setRequired(false);
		options.addOption(solr_base_url_opt);
		
		Option read_only_opt = new Option("r", "read-only", false, 
				"Used to initiate a run where the files are all read in, but nothing is ingested/saved");
		read_only_opt.setRequired(false);
		options.addOption(read_only_opt);
		
		// Need to work with CLI v1.2 as this is the JAR that is bundled with Hadoop/Spark 
		CommandLineParser parser = new GnuParser(); 
		//CommandLineParser parser = new DefaultParser(); // if working with CLI v1.3 and above
		
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		}
		catch (ParseException e) {
			System.err.println(e.getMessage());
			print_usage(formatter,options);
			System.exit(1);
		}

		
		String verbosity_str = cmd.getOptionValue("verbosity","1");
		int verbosity = Integer.parseInt(verbosity_str);

		String property_filename = cmd.getOptionValue("properties",null);
	
		String output_dir = cmd.getOptionValue("output-dir",null);
		String solr_base_url   = cmd.getOptionValue("solr-base-url",null);
		boolean read_only   = cmd.hasOption("read-only");
		
		String[] filtered_args = cmd.getArgs();

		if (filtered_args.length != 3) {
			print_usage(formatter,options);
			System.exit(1);
		}
	
		if (property_filename != null) {
			try {
				FileInputStream fis = new FileInputStream(property_filename);
				BufferedInputStream bis = new BufferedInputStream(fis);
				
				System.getProperties().load(bis);
			} 
			catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println("File not found: '" + property_filename + "'. Skipping property file read");
			}
			catch (IOException e) {
				System.err.println("IO Exception for: '" + property_filename + "'. Malformed syntax? Skipping property file read");
			}
		}
		
		if (!read_only && ((output_dir == null) && (solr_base_url==null))) {
			System.err.println("Need to specify either --solr-base-url or --output-dir otherwise generated files are not ingested/saved");
			print_usage(formatter,options);
			System.exit(1);
		}
		if (read_only) {
			// For this case, need to ensure solr-url and output-dir are null
			output_dir = null;
			solr_base_url = null;
		}
		
		String input_dir  = filtered_args[0];
		String json_file_list = filtered_args[1];
		String solr_collection = filtered_args[2];
		
		ProcessForSolrIngestJSONFilelist prep_for_ingest 
			= new ProcessForSolrIngestJSONFilelist(input_dir,json_file_list, 
												   solr_collection,solr_base_url, 
												   output_dir,verbosity);
			
		prep_for_ingest.execPerVolumeJSONFilelist();
		
	}
}
