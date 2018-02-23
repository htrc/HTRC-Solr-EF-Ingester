package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import org.apache.commons.cli.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;

import org.apache.spark.SparkConf;

/*
import com.mongodb.spark.api.java.MongoSpark;
import org.bson.Document;
import static java.util.Arrays.asList;
*/
public class ProcessForMongoDBIngest implements Serializable
{
	private static final long serialVersionUID = 1L;

	protected static final int DEFAULT_FILES_PER_PARTITION = 3000;
	
	protected String _input_dir;
	protected String _json_list_filename;
	
	protected int    _verbosity;

	public ProcessForMongoDBIngest(String input_dir, String json_list_filename, int verbosity)
	{
		_input_dir = input_dir;
		_json_list_filename = (json_list_filename != null) ? json_list_filename : input_dir;

		_verbosity  = verbosity;
	}

	protected String generateSparkAppName(String exec_mode)
	{
		String spark_app_name = "[" + exec_mode + "] Extracted Features: Process for MongoDB Ingest";
		spark_app_name += " [" + _json_list_filename + "]";
	
		return spark_app_name;
	}
		
	public void execMongoDBIngest()
	{	
		String spark_app_name = generateSparkAppName("Per Volume");		
		
		SparkConf spark_conf = new SparkConf().setAppName(spark_app_name);
		//spark_conf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/htrc_ef.volumes");
		
		JavaSparkContext jsc = new JavaSparkContext(spark_conf);

		/*
		JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
		        (new Function<Integer, Document>() {
		    public Document call(final Integer i) throws Exception {
		        return Document.parse("{test: " + i + "}");
		    }
		});

		MongoSpark.save(documents);
		*/
		
		String filename_root = _json_list_filename.replaceAll(".*/","").replaceAll("\\..*$","");
		String output_directory = "catalog-lang-" + filename_root + "-out";
		if (ClusterFileIO.exists(output_directory))
		{
			System.err.println("Error: " + output_directory + " already exists.  Spark unable to write output data");
			jsc.close();
			System.exit(1);
		}
		
		int files_per_partition = Integer.getInteger("wcsa-ef-ingest.files-per-partition", DEFAULT_FILES_PER_PARTITION);
		
		JavaRDD<String> json_list_data = jsc.textFile(_json_list_filename).cache();
		json_list_data.setName("JSON-file-list");
		
		long num_volumes = json_list_data.count();
		double per_vol = 100.0/(double)num_volumes;

		int num_partitions = (int)(num_volumes/files_per_partition)+1;
		
		JavaRDD<String> json_list_data_rp = json_list_data.repartition(num_partitions);
		json_list_data_rp.setName("JSON-file-list--repartitioned");
		
		DoubleAccumulator per_vol_progress_accum = jsc.sc().doubleAccumulator("Per Volume Progress Percent");
		
		boolean strict_file_io = Boolean.getBoolean("wcsa-ef-ingest.strict-file-io");
		
		PerVolumeMongoDBDocumentsMap volume_mongodb_docs_map 
			= new PerVolumeMongoDBDocumentsMap(_input_dir,_verbosity, 
								     per_vol_progress_accum,per_vol,
								     strict_file_io);
		JavaRDD<Integer> volume_page_counts = json_list_data_rp.map(volume_mongodb_docs_map); 
		volume_page_counts.persist(StorageLevel.MEMORY_AND_DISK());
		volume_page_counts.setName("volume-page-counts");
	
		Integer total_page_count = volume_page_counts.reduce((a, b) -> a + b);
		jsc.close();
		
		System.out.println("################");
		System.out.println("# Total Volume Count = " + total_page_count);
		System.out.println("################");
	}

	
	public static void print_usage(HelpFormatter formatter, Options options)
	{
		formatter.printHelp("RUN.bash [options] input-dir json-filelist.txt", options);
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
		
		String[] filtered_args = cmd.getArgs();

		if (filtered_args.length != 2) {
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
				e.printStackTrace();
				System.err.println("File not found: '" + property_filename + "'. Skipping property file read");
			}
			catch (IOException e) {
				System.err.println("IO Exception for: '" + property_filename + "'. Malformed syntax? Skipping property file read");
			}
		}
				
		String input_dir  = filtered_args[0];
		String json_list_filename = filtered_args[1];
		
		ProcessForMongoDBIngest process_mongodb_ingest 
			= new ProcessForMongoDBIngest(input_dir,json_list_filename,verbosity);
		
		process_mongodb_ingest.execMongoDBIngest();
	
	}
}
