/**
  * Created by Alina on 28.06.2016.
  */
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/**
  * colocation_pattern
  *
  */
object FPGrowth {

  /*Init Spark Context*/
  def initSparkContext(jars_directory:String):Tuple2[SparkContext,SQLContext] =
  {
    /*  Initialize Spark Context*/
    val conf: SparkConf = new SparkConf()
      .setAppName("FPGrowth")
      .setMaster("spark://10.114.22.10:7077")
      .set("spark.executor.memory", "100g")
      .set("spark.driver.memory", "100g")
      .set("spark.driver.maxResultSize","100g")
      .set("spark.logConf","true")

    val sc = new SparkContext(conf)
    sc.addJar("C:\\Users\\lws4\\Documents\\Scripts\\Master's Project\\colocationFPG\\target\\spatial-1.0-SNAPSHOT.jar")
    sc.addJar("hdfs://10.114.22.10:9000/alina/jars/commons-csv-1.1.jar")
    sc.addJar("hdfs://10.114.22.10:9000/alina/jars/spark-csv_2.10-1.4.0.jar")
    sc.addJar("hdfs://10.114.22.10:9000/alina/jars/univocity-parsers-1.5.1.jar")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    return (sc,sqlContext)

  }

  def toFlatTuple(tuple:Tuple2[Long,Array[String]]): TraversableOnce[Tuple2[Long,String]] =
  {
    var result:Array [Tuple2[Long,String]]=Array()
    val id=tuple._1
    for(rl<-tuple._2)
      {
        result:+=(id,rl)
      }

    return result

  }

/*Join EventTypes With Cameo Codes*/
  def joinWithCameoCODES(sqlContext:SQLContext,DF:DataFrame,colName:String,CAMEO_DF:DataFrame): DataFrame =
{
  //Join with CAMEO codes
    val result=DF
      .join(CAMEO_DF,DF.col(colName)===CAMEO_DF.col("CAMEOcode"))

    return result
  }

/*Read Cameo Codes*/

  def readCameoCodes(sQLContext: SQLContext,CAMEO_input:String):DataFrame= {
    //CAMEOcodes
    var CAMEOscheme = StructType (Array (
    StructField ("CAMEOcode", StringType, false),
    StructField ("EventDescription", StringType, false) ) )

    val CAMEO_codes = sQLContext.read
    .format ("com.databricks.spark.csv")
    .option ("header", "true")
    .option ("delimiter", "\t")
    .option ("nullValue", "")
    .option ("treatEmptyValuesAsNulls", "true")
    .schema (CAMEOscheme)
    .load (CAMEO_input)


    return CAMEO_codes

}

  /*Read Gdelt Events*/
  def readGDELTEvents(sQLContext: SQLContext,events_input:String):DataFrame=
  {
    var gdeltSchema = StructType(Array(
    StructField("EventCode", StringType, true),
    StructField("GLOBALEVENTID", IntegerType, true),
    StructField("ActionGeo_Long", FloatType, true),
    StructField("ActionGeo_Lat", FloatType, true)))

    val gdelt=sQLContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("nullValue", "")
    .option("treatEmptyValuesAsNulls", "true")
    .schema(gdeltSchema)
    .load(events_input)


  return  gdelt


  }
/*Join with Events*/
  def joinRulesWithEvents(sqlContext:SQLContext,EventsDF:DataFrame,RulesDF:DataFrame,colName:String):DataFrame=
  {

    val result=EventsDF.join(RulesDF,RulesDF.col(colName)===EventsDF.col("EventCode"))

  return result

  }

  /*Save Contextual Rules*/
  def saveContextualRules(sqlContext:SQLContext,antescentDF:DataFrame,conseqDF:DataFrame,output:String) =
  {
    val input= "hdfs://10.114.22.10:9000/alina/CameoCodes/CAMEO_event_codes.csv"
    val CAMEO=readCameoCodes(sqlContext,input)

    //Join Antescend with CAMEO codes
    val txtAntescend=joinWithCameoCODES(sqlContext,antescentDF,"antec",CAMEO)

    val a= txtAntescend.select("rule_id_a","EventDescription")
      .rdd.map(t=>(t(0),t(1)))
      .groupByKey()
      .map(t=>(t._1,t._2.mkString("[", ",", "]")))

    //Join Consequent with CAMEO codes
    val txtConseq=joinWithCameoCODES(sqlContext,conseqDF,"conseq",CAMEO)
      .withColumnRenamed("EventDescription","EventDescription1")

    val c= txtConseq.select("rule_id_c","EventDescription1")
      .rdd.map(t=>(t(0),t(1)))
      .groupByKey()
      .map(t=>(t._1,t._2.mkString("[", ",", "]")))

    val lines=a.cogroup(c)
      .map(t=>"Rule "+t._1.toString+": "+t._2._1.toList.mkString(";")+"==>"+t._2._2.toList.mkString(";"))
    lines.coalesce(1).saveAsTextFile(output)

  }

/*Extract Events for Rule*/
  def findEventsForRules(sqlContext:SQLContext,antescentDF:DataFrame,conseqDF:DataFrame,inputEvents:String,output:String) =
  {
    val input= "hdfs://10.114.22.10:9000/alina/CameoCodes/CAMEO_event_codes.csv"
    //read dataset
    val gdelt=readGDELTEvents(sqlContext,inputEvents)
    val CAMEO=readCameoCodes(sqlContext,input)

     /*Join with Origin Dataset*/

    val antescentEvents=joinRulesWithEvents(sqlContext,gdelt,antescentDF,"antec")
    val conseqEvents=joinRulesWithEvents(sqlContext,gdelt,conseqDF,"conseq")

    /*Join with CAMEO codes*/

    val antEvents=joinWithCameoCODES(sqlContext,antescentEvents,"EventCode",CAMEO)
      .select("rule_id_a","EventCode","EventDescription","GLOBALEVENTID","ActionGeo_Long","ActionGeo_Lat")
      .sort("rule_id_a")

    val conEvents=joinWithCameoCODES(sqlContext,conseqEvents,"EventCode",CAMEO)
      .select("rule_id_c","EventCode","EventDescription","GLOBALEVENTID","ActionGeo_Long","ActionGeo_Lat")
      .sort("rule_id_c")

    /*Save events for each rule to separate file*/
    antEvents
      .write
      .partitionBy("rule_id_a")
      .json(output+"/colocationRules/rulesEventsAntesc")

    conEvents
      .write
      .partitionBy("rule_id_c")
      .json(output+"/colocationRules/rulesEventsConseq")


  }

  def main(args: Array[String]) {


    /*HDFS directory*/
   val HDFS_directory= "hdfs://10.114.22.10:9000/alina"


   /*Init Spark Context*/
   val (sc,sqlContext)=initSparkContext(HDFS_directory+"/jars/")

  /*Read Transactions*/
   val data = sc.textFile(HDFS_directory+"/colocation/reference/transactions/part-00000")
   val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))



//----------------FPGrowth----------------------------------//
    val FPG = new FPGrowth()
      .setMinSupport(0.6)


    val model = FPG.run(transactions)
    model.freqItemsets.take(1000).foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    val minConfidence = 0.8
    val rules=model.generateAssociationRules(minConfidence)

    rules.map(rule => rule.antecedent.mkString("[", ",", "]")
      + " => " + rule.consequent .mkString("[", ",", "]")
      + ", " + rule.confidence)
      .saveAsTextFile(HDFS_directory+"/colocation/reference/colocation_rules/event_types_rules")

    val output_rules=HDFS_directory+"/colocation/reference/colocation_rules/txt_colocation_rules"


    val rules_pairs=rules.zipWithUniqueId().map(r=>Tuple2(r._2,r._1))
    rules_pairs.sortByKey()

    //transform rules to key value pairs
    val antTuples=rules_pairs.map(s=>(s._1,s._2.antecedent)).flatMap(s=>toFlatTuple(s)).sortByKey()
    val conseqTuples=rules_pairs.map(n=>(n._1,n._2.consequent)).flatMap(n=>toFlatTuple(n)).sortByKey()


    val antRows=antTuples.map(t=>Row(t._1,t._2))
    val conRows=conseqTuples.map(t=>Row(t._1,t._2))

    //create dataframes
    var ant_scheme = StructType(Array(
      StructField("rule_id_a", LongType, false),
      StructField("antec", StringType, false)))

    var cons_scheme = StructType(Array(
      StructField("rule_id_c", LongType, false),
      StructField("conseq", StringType, false)))


    val antescentDF=sqlContext.createDataFrame(antRows,ant_scheme)
    val conseqDF=sqlContext.createDataFrame(conRows,cons_scheme)

   //find contextual rules, join with CAMEO
    saveContextualRules(sqlContext,antescentDF,conseqDF,output_rules)

    val input_events=HDFS_directory+"/colocation/reference/gdeltReduced/part-00000"

    //find events
    findEventsForRules(sqlContext,antescentDF,conseqDF,input_events,output_rules)


      sc.stop();
  }

}
