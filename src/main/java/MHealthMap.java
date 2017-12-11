/*
  Polytech 5A - Big Data/Hadoop
	Année 2015/2016
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  WCountMap.java: classe MAP.
*/

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;

import java.lang.reflect.Method;



// Notre classe MAP.
public class MHealthMap extends Mapper<Object, Text, Text, FloatWritable> {

    // La fonction MAP elle-même.
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	
	// On récupère le nom de fichier de l'instance context: Afin de savoir qu'il s'agit de quel patient
        String fileName = "";
	InputSplit split = context.getInputSplit();
    	Class<? extends InputSplit> splitClass = split.getClass();

    	FileSplit fileSplit = null;
    	if (splitClass.equals(FileSplit.class)) {
      	  fileSplit = (FileSplit) split;

	//Your teacher add this line 
   	 fileName = fileSplit.getPath().getName();

       } else if (splitClass.getName().equals(
            "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
    	    // begin reflection hackery...

    	    try {
        	    Method getInputSplitMethod = splitClass
       	             .getDeclaredMethod("getInputSplit");
        	    getInputSplitMethod.setAccessible(true);
         	   fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
      	  } catch (Exception e) {
       	     // wrap and re-throw error
       	     throw new IOException(e);
       	 }

       	 // end reflection hackery

	//Your teacher add this line
   	 fileName = fileSplit.getPath().getName();
    }




        // La méthode split nous permet de récupérer toutes les valeurs dans un tableau String

	String[] fields = value.toString().split(" ");
	int n = fields.length;

	String activite_id = fields[n-1];

	

	int col_id=0;

        while (col_id < n) {
            float val = Float.parseFloat(fields[col_id]);

	    col_id++;
	    
            // On renvoie notre couple (clef;valeur)
            context.write(new Text("Patient index "+ fileName + " : Act " + activite_id + " - Col " + col_id + " * "), new FloatWritable(val));

        }
    }
}
