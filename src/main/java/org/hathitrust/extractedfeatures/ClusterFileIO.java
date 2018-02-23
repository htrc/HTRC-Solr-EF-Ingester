package org.hathitrust.extractedfeatures;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

public class ClusterFileIO {


	public static void memory_usage(String prefix)
	{
	    Runtime runtime = Runtime.getRuntime();
		
	    java.text.NumberFormat format = java.text.NumberFormat.getInstance();
	    
	    StringBuilder sb = new StringBuilder();
	    long maxMemory = runtime.maxMemory();
	    long allocatedMemory = runtime.totalMemory();
	    long freeMemory = runtime.freeMemory();
		
	    sb.append(prefix+" free memory: " + format.format(freeMemory / 1024) + "\n");
	    sb.append(prefix+" allocated memory: " + format.format(allocatedMemory / 1024) + "\n");
	    sb.append(prefix+" max memory: " + format.format(maxMemory / 1024) + "\n");
	    sb.append(prefix+" total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024) + "\n");

	    System.out.print(sb.toString());
	}

	
	protected static FileSystem getFileSystemInstance(String input_file_or_dir)
	{
		FileSystem fs = null;

		try {
			Configuration conf = new Configuration();
			URI uri = new URI(input_file_or_dir);
			fs = FileSystem.newInstance(uri,conf);
		} 
		catch (URISyntaxException e) {
			e.printStackTrace();	
		} 
		catch (IOException e) {
			e.printStackTrace();
		}

	    return fs;
	}
	
	public static boolean isHDFS(String fileIn)
	{
	 return fileIn.startsWith("hdfs://");
	}
	
	public static boolean exists(String file) 
	{
		FileSystem fs = getFileSystemInstance(file);
		
		boolean exists = false;
		
		try {
			Path path = new Path(file);
			exists = fs.exists(path);
		} catch (IllegalArgumentException e) {
			exists = false;
		} catch (IOException e) {
			exists = false;
		}

	    return exists;
	}
	
	public static String removeSuffix(String file,String suffix)
	{
		return file.substring(0,file.length() - suffix.length());
	}
	
	public static boolean createDirectoryAll(String dir) 
	{
		FileSystem fs = getFileSystemInstance(dir);
		boolean created_dir = false;

		if (!exists(dir)) {
			try {
				URI uri = new URI(dir);
				Path path = new Path(uri);
				fs.mkdirs(path);
				created_dir = true;	
			} catch (URISyntaxException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return created_dir;
	}
	
	public static BufferedInputStream getBufferedInputStream(String fileIn) 
			throws IOException 
	{
		FileSystem fs = getFileSystemInstance(fileIn);
		
	    BufferedInputStream bis = null;
	    
	    if (isHDFS(fileIn)) {
	    	URI uri = URI.create (fileIn);
	    	//Configuration conf = new Configuration();
	    	//FileSystem file = FileSystem.get(uri, conf);
	    	//FSDataInputStream fin = file.open(new Path(uri));
	    	
	    	//FSDataInputStream fin = _fs.open(new Path(fileIn));
	    	
	    	Path path = new Path(uri);
	    	FSDataInputStream fin = fs.open(path);
	    	
	    	bis = new BufferedInputStream(fin);
	    }
	    else {
	    	
	    	
	    	// Trim 'file://' off the front
	    	/*
	    	String local_file_in = fileIn;
	    	if (local_file_in.startsWith("file://")) {
	    		local_file_in = fileIn.substring("file://".length());
	    	}
	    	else if (local_file_in.startsWith("file:/")) {
	    		local_file_in = fileIn.substring("file:/".length());
	    	}
	    	FileInputStream fin = new FileInputStream(local_file_in);
	    	bis = new BufferedInputStream(fin);
	    	*/
	    	
	    	
	    	URI uri = URI.create (fileIn);
	    	Path path = new Path(uri);
	    	
	    	FSDataInputStream fin = fs.open(path);
	    	bis = new BufferedInputStream(fin);
	    	
	    	
	    }
	
	    return bis;
	}

	protected static String readTextFile(String filename)
	{
		StringBuilder sb = new StringBuilder();
		
		try {	
			BufferedReader br = ClusterFileIO.getBufferedReaderForCompressedFile(filename);

			int cp;
			while ((cp = br.read()) != -1) {
			    sb.append((char) cp);
			}
	
	        br.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return sb.toString();
	}
	
	public static BufferedOutputStream getBufferedOutputStream(String fileOut) 
			throws IOException 
	{
	    BufferedOutputStream bos = null;
	    
	    if (fileOut.startsWith("hdfs://")) {
	    	URI uri = URI.create (fileOut);
	    	Configuration conf = new Configuration();
	    	FileSystem file = FileSystem.get(uri, conf);
	    	FSDataOutputStream fout = file.create(new Path(uri));
	
	    	bos = new BufferedOutputStream(fout);
	    }
	    else {
	    	// Trim 'file://' off the front
	    	String local_file_out = fileOut;
	    	if (local_file_out.startsWith("file://")) {
	    		local_file_out = fileOut.substring("file://".length());
	    	}
	    	FileOutputStream fout = new FileOutputStream(local_file_out);
	    	bos = new BufferedOutputStream(fout);
	    }
	
	    return bos;
	}
	
	public static BufferedReader getBufferedReaderForCompressedFile(String fileIn) 
			throws IOException, CompressorException 
	{
	    BufferedInputStream bis = getBufferedInputStream(fileIn);
	    CompressorInputStream cis = new CompressorStreamFactory().createCompressorInputStream(bis);
	    BufferedReader br = new BufferedReader(new InputStreamReader(cis,"UTF8"));
	    return br;
	}

	public static BufferedWriter getBufferedWriterForCompressedFile(String fileOut) 
			throws IOException, CompressorException 
	{
	    BufferedOutputStream bos = getBufferedOutputStream(fileOut);
	    CompressorStreamFactory csf = new CompressorStreamFactory();
	    CompressorOutputStream cos = csf.createCompressorOutputStream(CompressorStreamFactory.BZIP2,bos);
	    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(cos,"UTF8"));
	    return bw;
	}

}
