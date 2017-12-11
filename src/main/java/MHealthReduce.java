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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;



// Notre classe REDUCE - templatée avec un type générique K pour la clef, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class MHealthReduce extends Reducer<Text, FloatWritable, Text, Text> {


    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

	MongoClient mongoClient=new MongoClient();
	MongoDatabase db=mongoClient.getDatabase(Constants.DATABASE_NAME);
	MongoCollection collection=db.getCollection(Constants.COLLECTION_NAME);

	// key * value recu du Map exemple :
	// Patient index mHealth_subject1.log : Act 1 - Col 4 *  * 0.0027908671	
	String myKey = 	key.toString();
	String activiteCol = myKey.substring(myKey.indexOf(":")+1, myKey.indexOf("*")).trim();
	String index = myKey.substring(myKey.indexOf("P"), myKey.indexOf(":")).trim();

	String myString ="";

	float cardio = 0f;
	
	boolean testAct = false;
	boolean testCardio = true;


	//Dans notre exemple la liste Iterable contient une seule valeur float pour la même clé
        Iterator<FloatWritable> i = values.iterator();
	cardio = i.next().get();



	switch (activiteCol) {

         case "Act 1 - Col 4":
		if (cardio > Constants.FAKE_SEUIL1 || cardio < Constants.FAKE_SEUIL2)
	      	   testCardio = false;   
		testAct=true; 
		myString=Constants.ACT1_COL4;         
             break;


         case "Act 1 - Col 5":
		if (cardio > Constants.FAKE_SEUIL1 || cardio < Constants.FAKE_SEUIL2)
	      	   testCardio = false;   
		testAct=true; 
		myString=Constants.ACT1_COL5;
             break;


         case "Act 5 - Col 4":
		if (cardio > Constants.FAKE_SEUIL1 || cardio < Constants.FAKE_SEUIL2)
	      	   testCardio = false;   
		testAct=true;               
		myString=Constants.ACT5_COL4;
             break;


         case "Act 5 - Col 5":
		if (cardio > Constants.FAKE_SEUIL1 || cardio < Constants.FAKE_SEUIL2)
	      	   testCardio = false;   
		testAct=true;              
		myString=Constants.ACT5_COL5;
             break;


         case "Act 10 - Col 4":
		if (cardio > Constants.FAKE_SEUIL1 || cardio < Constants.FAKE_SEUIL2)
	      	   testCardio = false;   
		testAct=true;
		myString=Constants.ACT10_COL4;
             break;


         case "Act 10 - Col 5":
		if (cardio > Constants.FAKE_SEUIL1 || cardio < Constants.FAKE_SEUIL2)
	      	   testCardio = false;   
		testAct=true;         
		myString=Constants.ACT10_COL5;
             break;


         //default:
     }

	String finalKey = index + " "+ myString +  " " + activiteCol + " - valeur cardio " + cardio;

	if (testCardio && testAct) {

	  collection.insertOne(new Document().append("index",finalKey).append("decision",Constants.DECISION_OK));

          context.write(new Text(finalKey), new Text(Constants.DECISION_OK));
	}

	if (!testCardio && testAct) {	

	  collection.insertOne(new Document().append("index",finalKey).append("decision",Constants.DECISION_NOTOK));

          context.write(new Text(finalKey), new Text(Constants.DECISION_NOTOK));
	}
                  
      
    }

}
