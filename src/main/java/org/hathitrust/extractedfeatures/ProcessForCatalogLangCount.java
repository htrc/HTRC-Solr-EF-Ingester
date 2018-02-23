package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;

import org.apache.spark.SparkConf;

public class ProcessForCatalogLangCount implements Serializable
{
	private static final long serialVersionUID = 1L;

	protected static final int DEFAULT_FILES_PER_PARTITION = 3000;
	
	protected String _input_dir;
	protected String _json_list_filename;
	
	protected int    _verbosity;

	public ProcessForCatalogLangCount(String input_dir, String json_list_filename, int verbosity)
	{
		_input_dir = input_dir;
		_json_list_filename = (json_list_filename != null) ? json_list_filename : input_dir;

		_verbosity  = verbosity;
	}

	protected String generateSparkAppName(String exec_mode)
	{
		String spark_app_name = "[" + exec_mode + "] Extracted Features: Process for Catalog Language Labels";
		spark_app_name += " [" + _json_list_filename + "]";
	
		return spark_app_name;
	}
		
	public void execCatalogLangCountSparkDirect()
	{	
		SparkConf conf = new SparkConf().setAppName("Spark-Direct + Per Volume");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		String filename_root = _json_list_filename.replaceAll(".*/","").replaceAll("\\..*$","");
		String output_directory = "catalog-lang-" + filename_root + "-out";
		if (ClusterFileIO.exists(output_directory))
		{
			System.err.println("Error: " + output_directory + " already exists.  Spark unable to write output data");
			jsc.close();
			System.exit(1);
		}
		
		//int num_partitions = Integer.getInteger("wcsa-ef-ingest.num-partitions", DEFAULT_NUM_PARTITIONS);
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
		
		PerVolumeCatalogLangStreamFlatmap volume_catalog_langfreq_flatmap 
			= new PerVolumeCatalogLangStreamFlatmap(_input_dir,_verbosity, 
								     per_vol_progress_accum,per_vol,
								     strict_file_io);
		JavaRDD<String> catalog_lang_list = json_list_data_rp.flatMap(volume_catalog_langfreq_flatmap); 
		catalog_lang_list.persist(StorageLevel.MEMORY_AND_DISK());
		catalog_lang_list.setName("catalog-lang-stream");
	
		
		JavaPairRDD<String, Long> catalog_lang_pairs = catalog_lang_list.mapToPair(s -> new Tuple2<String, Long>(s, 1L));
		catalog_lang_pairs.setName("single-catalog-lang-count");
		
		JavaPairRDD<String, Long> catalog_lang_counts = catalog_lang_pairs.reduceByKey((a, b) -> a + b);
		catalog_lang_counts.setName("catalog-lang-frequency");
		
		JavaPairRDD<Long, String> catalog_lang_counts_swapped_pair
			= catalog_lang_counts.mapToPair(item -> item.swap());
		catalog_lang_counts_swapped_pair.setName("frequency-catalog-lang-swap");
		
		JavaPairRDD<Long, String> catalog_lang_counts_swapped_pair_sorted 
			= catalog_lang_counts_swapped_pair.sortByKey(false, num_partitions);
		catalog_lang_counts_swapped_pair_sorted.setName("descending-sorted-frequency-catalog-lang");
		
		JavaPairRDD<String, Long> catalog_lang_count_sorted 
			= catalog_lang_counts_swapped_pair_sorted.mapToPair(item -> item.swap());
		catalog_lang_count_sorted.setName("descending-catalog-lang-frequency");
		
		
		catalog_lang_count_sorted.saveAsTextFile(output_directory);
		jsc.close();
	}

	public void sampleDown10000()
	{
		String spark_app_name = generateSparkAppName("Spark Cluster + Per Volume: Downsample 10000");		
		
		SparkConf conf = new SparkConf().setAppName(spark_app_name);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		jsc.hadoopConfiguration().set("io.compression.codec.bzip2.library", "java-builtin");
		
		String packed_sequence_path = "hdfs:///user/capitanu/data/packed-ef";

		JavaPairRDD<Text, Text> input_pair_rdd = jsc.sequenceFile(packed_sequence_path, Text.class, Text.class);

		JavaPairRDD<Text, Text> json_text_sample_rdd = input_pair_rdd.sample(false,0.0001,42);
		
		String output_directory = "packed-ef-10000";
		json_text_sample_rdd.saveAsTextFile(output_directory);
		
		
		jsc.close();
	}
	
	 public static class ConvertToWritableTypes implements PairFunction<Tuple2<Text, Text>, String, String> {
		    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(Tuple2<Text, Text> record) {
		      return new Tuple2(record._1.toString(), record._2.toString());
		    }
		  }
	 
	public void sampleDown100()
	{
		String spark_app_name = generateSparkAppName("Spark Cluster + Per Volume: Downsample 100");		
		
		SparkConf conf = new SparkConf().setAppName(spark_app_name);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		jsc.hadoopConfiguration().set("io.compression.codec.bzip2.library", "java-builtin");
		
		String packed_sequence_path = "hdfs:///user/capitanu/data/packed-ef";

		JavaPairRDD<Text, Text> input_pair_rdd = jsc.sequenceFile(packed_sequence_path, Text.class, Text.class);

		JavaPairRDD<Text, Text> json_text_sample_rdd = input_pair_rdd.sample(false,0.01,42);
		
		JavaPairRDD<Text, Text> json_text_sample_repart_rdd = json_text_sample_rdd.repartition(120);
		
		//JavaPairRDD<Text, Text> json_text_sample_repart_rdd = json_text_sample_rdd.repartition(120);
		
		String output_directory = "packed-full-ef-100";
		//json_text_sample_repart_rdd.saveAsTextFile(output_directory);
		//json_text_sample_repart_rdd.saveAsSequenceFile(output_directory);
		json_text_sample_repart_rdd.saveAsHadoopFile(output_directory, Text.class, Text.class, SequenceFileOutputFormat.class);
		
		//SequenceFileOutputFormat<Text,Text> sfof = new SequenceFileOutputFormat<Text,Text>();
		// //sfof.setOutputCompressionClass(BZip2Codec.class);
		// //sfof.setOutputCompressorClass(conf);
		// json_text_sample_repart_rdd.saveAsNewAPIHadoopFile(output_directory, Text.class, Text.class, sfof);
		//org.apache.hadoop.mapred.SequenceFileAsBinaryOutputFormat
		//org.apache.hadoop.mapred.
		//json_text_sample_repart_rdd.saveAsObjectFile(output_directory);
		
		//JavaPairRDD<String,String> result = json_text_sample_repart_rdd.mapToPair(new ConvertToWritableTypes());
	    //result.saveAsHadoopFile(output_directory, String.class, String.class, SequenceFileOutputFormat.class);
	    
		jsc.close();
	}
	public void execCatalogLangCount()
	{	
			
		String spark_app_name = generateSparkAppName("YARN Cluster + Per Volume");		
		
		SparkConf conf = new SparkConf().setAppName(spark_app_name);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		jsc.hadoopConfiguration().set("io.compression.codec.bzip2.library", "java-builtin");
		
		String packed_sequence_path = "hdfs:///user/capitanu/data/packed-ef";

		JavaPairRDD<Text, Text> input_pair_rdd = jsc.sequenceFile(packed_sequence_path, Text.class, Text.class);
		//JavaRDD<Text> jsonTextRdd = input_pair_rdd.map(Tuple2::_2);
		JavaRDD<Text> json_text_rdd = input_pair_rdd.map(item -> item._2);
		
		//JavaRDD<Text> json_text_sample_rdd = json_text_rdd.sample(false,0.0001);
		
		/*
		jsonTextRdd.map(
		    jsonText -> "" // parse JSON text and do whatever ...
		);
			*/
		
		boolean strict_file_io = Boolean.getBoolean("wcsa-ef-ingest.strict-file-io");
		
		PerVolumeCatalogLangSequenceFileMap volume_catalog_langfreq_map
			= new PerVolumeCatalogLangSequenceFileMap(_input_dir,_verbosity,strict_file_io);
		JavaRDD<String> catalog_lang_list = json_text_rdd.map(volume_catalog_langfreq_map); 
		//catalog_lang_list.persist(StorageLevel.MEMORY_AND_DISK());
		catalog_lang_list.setName("catalog-lang-stream");
	
		
		JavaPairRDD<String, Long> catalog_lang_pairs = catalog_lang_list.mapToPair(s -> new Tuple2<String, Long>(s, 1L));
		catalog_lang_pairs.setName("single-catalog-lang-count");
		
		JavaPairRDD<String, Long> catalog_lang_counts = catalog_lang_pairs.reduceByKey((a, b) -> a + b);
		catalog_lang_counts.setName("catalog-lang-frequency");
		
		JavaPairRDD<Long, String> catalog_lang_counts_swapped_pair
			= catalog_lang_counts.mapToPair(item -> item.swap());
		catalog_lang_counts_swapped_pair.setName("frequency-catalog-lang-swap");
		
		JavaPairRDD<Long, String> catalog_lang_counts_swapped_pair_sorted 
			= catalog_lang_counts_swapped_pair.sortByKey(false,20);
		catalog_lang_counts_swapped_pair_sorted.setName("descending-sorted-frequency-catalog-lang");
		
		JavaPairRDD<String, Long> catalog_lang_count_sorted 
			= catalog_lang_counts_swapped_pair_sorted.mapToPair(item -> item.swap());
		catalog_lang_count_sorted.setName("descending-catalog-lang-frequency");
		
		String filename_root = _json_list_filename.replaceAll(".*/","").replaceAll("\\..*$","");
		String output_directory = "catalog-lang-" + filename_root + "-out";
		catalog_lang_count_sorted.saveAsTextFile(output_directory);
		jsc.close();
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
		
		ProcessForCatalogLangCount prep_for_lang 
			= new ProcessForCatalogLangCount(input_dir,json_list_filename,verbosity);
		
		//prep_for_lang.execCatalogLangCount();
		prep_for_lang.sampleDown100();
	
	}
}
