/*
  Polytech 5A - Big Data/Hadoop
	Année 2015/2016
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  WCountMap.java: classe driver (contient le main du programme).
*/

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


// Note classe Driver (contient le main du programme Hadoop).
public class MHealth {
    // Le main du programme.
    public static void main(String[] args) throws Exception {
        // Créé un object de configuration Hadoop.
        Configuration conf = new Configuration();

        // Permet à Hadoop de lire ses arguments génériques, récupère les arguments restants dans ourArgs.
        String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Obtient un nouvel objet Job: une tâche Hadoop. On fourni la configuration Hadoop ainsi qu'une description
        // textuelle de la tâche.
        Job job = Job.getInstance(conf, "MHealth v1.0");

        // Défini les classes driver, map et reduce.
        job.setJarByClass(MHealth.class);
        job.setMapperClass(MHealthMap.class);
	job.setCombinerClass(MHealthCombiner.class);
        job.setReducerClass(MHealthReduce.class);

        // sorties du mapper = entrées du reducer et du combiner
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

 

	// Défini les fichiers d'entrée du programme et le répertoire des résultats.
        // On se sert du premier, deuxième et troisième argument restants pour permettre à l'utilisateur de les spécifier
        // lors de l'exécution.
	Path p1=new Path(ourArgs[0]); 
	Path p2=new Path(ourArgs[1]);
 	Path p3=new Path(ourArgs[2]);
 	//FileSystem fs = FileSystem.get(c);
 	//if(fs.exists(p3)){
	  //fs.delete(p3, true);
  	//}

	//Dans notre cas les 2 fichiers d'entrée seront excuter par le meme Map
 	MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MHealthMap.class);
 	MultipleInputs.addInputPath(job,p2, TextInputFormat.class, MHealthMap.class);

	//Path de sortie
        FileOutputFormat.setOutputPath(job, p3);



        // On lance la tâche Hadoop. Si elle s'est effectuée correctement, on renvoie 0. Sinon, on renvoie -1.
        if (job.waitForCompletion(true))
            System.exit(0);
        System.exit(-1);
    }
}
