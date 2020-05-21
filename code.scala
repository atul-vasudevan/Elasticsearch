import scalaj.http._
import scala.xml.XML
import org.json4s._
import collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.json4s.Xml.{toJson, toXml}
//Object CaseIndex
object CaseIndex {
    //ElasticSearch server
    val esServer = "http://localhost:9200" 
    //Calling the NLP Server 
    def callNLP(xmlFile:String): String = {
        val nlpServer = "http://localhost:9000"
        val response = Http(nlpServer).postData(xmlFile).param("annotators","ner").param("ner.applyFineGrained","false").param("outputFormat","xml").timeout(connTimeoutMs = 10000 , readTimeoutMs = 100000).asString
        return response.body
    }
    //Function to extract the NEW and word tags
    def extract(xmlF:String): (scala.collection.mutable.Set[String], scala.collection.mutable.Set[String], scala.collection.mutable.Set[String])= {
            var setLocation = scala.collection.mutable.Set[String]()
            var setPerson = scala.collection.mutable.Set[String]()
            var setOrganization = scala.collection.mutable.Set[String]()
            val readXML =XML.loadString(xmlF)
            //Get the NER tag values
            val ner = readXML \\ "NER"
            //Get the word tag values
            val word = readXML \\ "word"
            for((n,i) <- ner.view.zipWithIndex){
                if (n.text == "LOCATION"){
                    //Store the location values in a Set
                    setLocation+=word(i).text
                }
                if (n.text == "PERSON"){
                    //Store the person values in a Set
                    setPerson+=word(i).text
                }
                if (n.text == "ORGANIZATION"){
                    //Store the organization values in a Set
                    setOrganization+=word(i).text
                }  
            } 
            return (setLocation,setPerson,setOrganization) 
        }
    //Function where we create the document
    def documentCreation(name:String,content:String,location:String,person:String,organization:String): String = {
        //replace the newline with a space
        val newContent=content.replace("\n"," ")
        val document_string = "{\"fileName\" :  \"" + name + "\", \"location\" :  \"" + location + "\", \"person\" : \"" + person + "\", \"organization\" : \"" + organization + "\", \"content\" : \"" + newContent + "\"}"
        //calling the elastic server and creating a document which will be queried later
        val createDocument = Http(esServer + "/legal_idx/cases?pretty").header("content-type", "application/json").postData(document_string).method("POST").asString
        return createDocument.body
    }
    def main(args: Array[String]) {
        val inputFile = args(0)
    val conf = new SparkConf().setAppName("CaseIndex").setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    //Reading the xml files using wholeTextFile so that we get the name and content of the xml file
    val xmlFiles = sc.wholeTextFiles(args(0)) 
    val convertedXml = xmlFiles.map{case(name,content) => (name,content,XML.loadString(content))}
    //Calling the coreNLP server
    val output = convertedXml.map{case(name,content,xml) => (name,content,callNLP(xml.text))}
    //Calling the extract function where we extract the NER and word tags
    val out= output.map{case(name,content,xml) => (name,content,extract(xml))}
    //Create an index "legal_idx" in the elastic server
    val responseElasticSearch = Http(esServer + "/legal_idx?pretty").method("PUT").timeout(connTimeoutMs = 10000 , readTimeoutMs = 50000).asString
    //println("Index creation ----"+responseElasticSearch.body)
    //The schema which we are using to add content into the document
    val mapping = "{\"cases\": {\"properties\": {\"fileName\": {\"type\": \"text\"},\"location\": {\"type\": \"text\"},\"person\": {\"type\": \"text\"}, \"organization\": {\"type\": \"text\"},\"content\": {\"type\": \"text\"}}}}"
    //Calling the elastic server and creating the mapping
    val elasticSearchMapping = Http(esServer + "/legal_idx/cases/_mapping?pretty").header("content-type", "application/json").postData(mapping).method("POST").timeout(connTimeoutMs = 10000 , readTimeoutMs = 50000).asString
    //println("Mapping creation ----"+elasticSearchMapping.body)
    //Convertiing the sets to string and sending it document creation function.
    //We replace " with \". We basically escape the quotes in order to maintain the string integrity
    val dataES = out.map{case(name,content,(loc,per,org)) => (documentCreation(name,content.replace("\"","\\\""),loc.mkString(" "),per.mkString(" "),org.mkString(" ")))}
    
    dataES.map(a=>a).saveAsTextFile("output")
    //If the above fails, please try the below command
    //dataES.foreach(a => a)
    }
}
