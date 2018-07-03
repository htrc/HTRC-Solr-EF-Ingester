package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONArray;
import org.json.JSONObject;

public class ProcessForConceptIncrementalIngest 
{
	//private static final long serialVersionUID = 1L;
	
	protected static final int DEFAULT_NUM_CORES = 10;
	protected static final int MINIMUM_NUM_PARTITIONS = 10*DEFAULT_NUM_CORES; 
	
	protected static final int DEFAULT_FILES_PER_PARTITION = 3000;
	
	protected String _json_concept_oneliners_filename;
	protected String _solr_base_url;
	protected String _solr_collection;
	
	//protected String _whitelist_filename;
	//protected String _langmap_directory;
	
	protected int    _verbosity;

	public ProcessForConceptIncrementalIngest(String json_oneliners, String solr_collection,
									  		  String solr_base_url, int verbosity)
	{
		_json_concept_oneliners_filename = json_oneliners;
		_solr_collection = solr_collection;
		/*
		boolean use_whitelist = Boolean.getBoolean("wcsa-ef-ingest.use-whitelist");
		_whitelist_filename = (use_whitelist) ?  System.getProperty("wcsa-ef-ingest.whitelist-filename") : null;
		
		boolean use_langmap = Boolean.getBoolean("wcsa-ef-ingest.use-langmap");
		_langmap_directory = (use_langmap) ?  System.getProperty("wcsa-ef-ingest.langmap-directory") : null;
		*/
		
		_solr_base_url   = solr_base_url;
		_verbosity  = verbosity;
	}

	protected String generateAppName(String level)
	{
		String app_name = "Capisco Concepts: Process for JSON one-liners Solr Incremental Ingest [" + level + "]";
		app_name += " [" + _solr_collection + "]";

		if (_solr_base_url != null) { 
			app_name += " solr_base_url=" + _solr_base_url;
		}
		
		return app_name;
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

	public ArrayList<String> readFileLines(Path list_path)
	{
		ArrayList<String> json_file_list = new ArrayList<String>();
		
		try (Stream<String> list_lines = Files.lines(list_path)) {
			list_lines.forEach(line -> {
				json_file_list.add(line);
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return json_file_list;
		
	}
	
	public Text readJSONText(Path json_path)
	{
		File json_file = json_path.toFile();
	
		String json_filename = json_file.toURI().toString();
		/*
		try {
			json_filename = json_file.getCanonicalPath();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		*/
		String text_string = ClusterFileIO.readTextFile(json_filename);
		
		//ArrayList<String> text_lines = readFileLines(json_path);
		
		//String text_string = String.join("\n",text_lines);
		
		Text json_text = new Text(text_string);
		return json_text;
		
	}
	
	public void execPerPageJSONFile()
	{
		String spark_app_name = generateAppName("Page");		
	
		SparkConf spark_conf = new SparkConf().setAppName(spark_app_name);
		JavaSparkContext jsc = new JavaSparkContext(spark_conf);
		
		int files_per_partition = Integer.getInteger("wcsa-ef-ingest.files-per-partition", DEFAULT_FILES_PER_PARTITION);
		
		JavaRDD<String> json_concepts_data = jsc.textFile(_json_concept_oneliners_filename).cache();
		json_concepts_data.setName("JSON-concept-onliners");
		
		long num_pages = json_concepts_data.count();
		
		int num_partitions = (int)(num_pages/files_per_partition)+1;
		if (num_partitions < MINIMUM_NUM_PARTITIONS) {
			num_partitions = MINIMUM_NUM_PARTITIONS;
		}
		
		JavaRDD<String> json_concepts_data_rp = json_concepts_data.repartition(num_partitions);
		json_concepts_data_rp.setName("JSON-concept-onliners--repartitioned");
		
		ArrayList<String> solr_endpoints = extrapolateSolrEndpoints(_solr_collection);
		
		PerPageConceptsJSON per_page_json_concepts = new PerPageConceptsJSON(solr_endpoints,_verbosity);
		
		JavaRDD<Integer> per_page_count = json_concepts_data_rp.map(per_page_json_concepts);
		per_page_count.setName("page-count");
		
		long num_page_ids = per_page_count.count();
		
		System.out.println("");
		System.out.println("############");
		System.out.println("# Number of page ids: " + num_page_ids);
		System.out.println("############");
		System.out.println("");

		jsc.close();
	}
	
	
	public static void print_usage(HelpFormatter formatter, Options options)
	{
		formatter.printHelp("_RUN-WITH-OPTIONS.bash [options] solr-collection", options);
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
		
		Option solr_base_url_opt = new Option("u", "solr-base-url", true, 
				"If specified, the base URL to post the Solr JSON data to");
		solr_base_url_opt.setRequired(false);
		options.addOption(solr_base_url_opt);
		
		Option read_only_opt = new Option("r", "read-only", false, 
				"Used to initiate a run where the files are all read in, but nothing is ingested/saved");
		read_only_opt.setRequired(false);
		options.addOption(read_only_opt);
		
		Option json_oneliners_opt = new Option("j", "json-oneliners", true, 
				"The file containing the concepts (one-per line in JSON format) to be processed");
		json_oneliners_opt.setRequired(true);
		options.addOption(json_oneliners_opt);
		
		// Need to work with CLI v1.2 as this is the JAR that is bundled with Hadoop/Spark 
		CommandLineParser parser = new GnuParser(); 
		//CommandLineParser parser = new DefaultParser(); // if working with CLI v1.3 and above (possible with shadowing)
		
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
		String solr_base_url     = cmd.getOptionValue("solr-base-url",null);
		boolean read_only        = cmd.hasOption("read-only");
		
		String json_oneliners = cmd.getOptionValue("json-oneliners",null);
		
		String[] filtered_args = cmd.getArgs();

		if (filtered_args.length != 1) {
			System.err.println("Expected single Solr collection name after optional minus arguments");
			print_usage(formatter,options);
			System.exit(1);
		}
	
		String solr_collection = filtered_args[0];
		
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
		
		if (read_only) {
			// If read_only ensure solr-url is null
			solr_base_url = null;
		}
		else if (solr_base_url==null) {
			// Not read_only, but no solr_base_url has been set
			System.err.println("Need to specify --solr-base-url otherwise no data is ingested");
			print_usage(formatter,options);
			System.exit(1);
		}

	
		
		ProcessForConceptIncrementalIngest prep_for_inc_ingest 
			= new ProcessForConceptIncrementalIngest(json_oneliners,solr_collection,solr_base_url,verbosity);
			
		prep_for_inc_ingest.execPerPageJSONFile();
		
		
	}
}
