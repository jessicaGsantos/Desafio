
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.Tuple2;
import java.util.Arrays;


public class Main{

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Challenge Spark").setMaster("local[2]").set("spark.executor.memory","1g");
	    SparkContext sc = new SparkContext(conf);
	    JavaRDD<String> textFileJuly = sc.textFile("/Users/jessicagomesdossantos/Desktop/Desafio Semantix/NASA_access_log_Jul95",0).toJavaRDD();
	    JavaRDD<String> textFileAugust = sc.textFile("/Users/jessicagomesdossantos/Desktop/Desafio Semantix/NASA_access_log_Aug95",0).toJavaRDD();
	    
	    //1. Número de hosts únicos.
	    /*esta questão ficou incompleta, pois não consegui entender exatamente como usar as funções para
	     *utilizar apenas os hosts para comparação, torná-los únicos e então contá-los
	     *Por conta disto, também não consegui resolver as questões 3, 4 e 5
	     *Gostaria porém de agradecer pelo desafio, pois eu não possuía nenhum conhecimento de Spark, e acabei
	     *aprendendo conceitos importantes que podem guiar meu aprendizado futuro no assunto */
	    
	    JavaRDD<String> lines = textFileJuly.flatMap(line -> Arrays.asList(line.split("\n")).iterator());
	    JavaPairRDD<String, Integer> counts = lines
	    		.mapToPair(w -> new Tuple2<String,Integer>(w, 1))
	    		.reduceByKey((x, k) -> k);
	    counts.saveAsTextFile("/Users/jessicagomesdossantos/Desktop/Desafio Semantix/result");
	    
	    //2. O total de erros 404.
	    JavaRDD<String> linesJuly = textFileJuly.filter(s -> s.contains("404"));
	    JavaRDD<String> linesAugust = textFileAugust.filter(s -> s.contains("404"));
	    long total = linesJuly.count()+linesAugust.count();
	    System.out.println("Errors 404 in July: "+linesJuly.count()+"\nErrors 404 in August: "+
	    linesAugust.count()+"\nTotal: "+total);
	    
	    //3. Os 5 URLs que mais causaram erro 404.
	    
	    
	    //4. Quantidade de erros 404 por dia.
	    
	    
	    //5. O total de bytes retornados.
	    
	    
	    /*código consultado da documentação do spark e das seguintes fontes:
	     * https://databricks.com/blog/2014/04/14/spark-with-java-8.html
	     * https://github.com/ypriverol/spark-java8/blob/master/src/main/java/org/sps/learning/spark/basic/SparkWordCount.java
	    */
		
	}

}
