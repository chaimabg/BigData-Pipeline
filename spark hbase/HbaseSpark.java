package tn.insat.project;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.hadoop.hbase.util.Bytes;
public class HbaseSpark  implements java.io.Serializable{
	
	static  PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String> mapFunc=new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
         public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
             
             System.out.println("--------------------------------------------------------------------------------");
             Result result = entry._2;
             String keyRow = Bytes.toString(result.getRow());
             String state = Bytes.toString(result.getValue(Bytes.toBytes("usa-covid"), Bytes.toBytes("states")));
             String cases = Bytes.toString(result.getValue(Bytes.toBytes("usa-covid"), Bytes.toBytes("cases")));
             
             return new Tuple2<String, String>(state,cases);
 }
     };
    public void createHbaseTable(String[] args) throws Exception{
    	 
        Configuration config = HBaseConfiguration.create();
        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest").setMaster("local[4]");
        
        
        JavaSparkContext jsc =  new JavaSparkContext();
        
        
        config.set(TableInputFormat.INPUT_TABLE,"covid");
       
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
       
              JavaPairRDD<String, String> rowPairRDD = hBaseRDD.mapToPair(mapFunc);
              rowPairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
  				public void call(Tuple2<String, String> t) throws Exception {
  					System.out.println(t._1+"-------------------------------------------------------------"+t._2);
  				}
  			});
        
       
       
    
      
    }

    
    	 public static void main(String[] args)  throws Exception{
    	       
    	       
    	HbaseSpark admin = new HbaseSpark();
        admin.createHbaseTable(args);
    }

}
