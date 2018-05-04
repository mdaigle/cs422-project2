package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    //**** PHASE 1 ****
    // Map
    val partials = rdd.map(r => format(r, index, indexAgg, agg))
    // combine locally, shuffle, and reduce
    .reduceByKey((a, b) => mergeValues(a, b, agg))
    // compute partial values
    .flatMap(p => getPartials(p))

    //**** PHASE 2 ****
    partials.reduceByKey((a, b) => mergeValues(a, b, agg))
      .map(p => (p._1, p._2._1))
  }

  def format(r: Row, index: List[Int], indexAgg:Int, agg: String): (String, (Double,Int)) = {
    val key:String = index.map(i => r.getString(i)).mkString(",")
    val value = agg match {
      case "COUNT" => (1.0, 0)
      case _ => (r.getDouble(indexAgg), 0)
    }

    (key, value)
  }

  def getPartials(p:(String, (Double, Int))):TraversableOnce[(String, (Double,Int))] = {
    val index = p._1.split(",")
    val permutations = math.pow(2, index.size).toInt - 1
    var out = new Array[(String, (Double,Int))](permutations)
    for (x <- 1 to permutations) {
      var partial_key:String = ""
      for (i <- 0 to index.size) {
        if ((x / math.pow(2, i) % 1) == 0) {
          partial_key = partial_key.concat(index(i))
        } else {
          partial_key = partial_key.concat("*")
        }
      }
      out(x - 1) = (partial_key, p._2)
    }

    out
  }

  def mergeValues(a:(Double,Int), b:(Double,Int), agg:String): (Double, Int) = {
    agg match {
      case "COUNT" => (a._1+b._1, 0)
      case "SUM" => (a._1+b._1, 0)
      case "MIN" => (math.min(a._1, b._1), 0)
      case "MAX" => (math.max(a._1, b._1), 0)
      case "AVG" => ((a._1*a._2 + b._1*b._2)/(a._2 + b._2), (a._2 + b._2))
    }
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    //TODO naive algorithm for cube computation
    null
  }

}
