/*
  Polytech 5A - Big Data/Hadoop
	Année 2015/2016
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  WCountReduce.java: classe REDUCE.
*/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;


/**
 * Cette classe effectue une réduction partielle à la sortie des Mappers sur une même machine
 */
public class MHealthCombiner
        extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    // La fonction REDUCE elle-même. Les arguments: la clef key (d'un type générique K), un Iterable de toutes les valeurs
    // qui sont associées à la clef en question, et le contexte Hadoop (un handle qui nous permet de renvoyer le résultat à Hadoop).
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        // Pour parcourir toutes les valeurs associées à la clef fournie.
        Iterator<FloatWritable> i = values.iterator();
        int count = 0;
	float sum = 0f;
        while (i.hasNext()) {  // Pour chaque valeur...
            sum += i.next().get();    // ...on l'ajoute au total.
	    count++;
	}

	float avg = (float) sum/count;

        // On renvoie le couple (clef;valeur) 
        context.write(key, new FloatWritable(avg));
    }
}


