package cubeoperator

import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

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
    val partials = rdd.filter(r => r.size == schema.size)
      .map { r =>
        val key: String = index.map(i => r.get(i).toString).mkString(",")
        val value = agg match {
          case "COUNT" => (1.0, 1)
          case _ => (if (r.get(indexAgg).isInstanceOf[Double]) r.getDouble(indexAgg) else r.getInt(indexAgg), 1)
        }

        (key, value)
      }
    // combine locally, shuffle, and reduce
    .reduceByKey((a, b) => agg match {
      case "COUNT" => (a._1+b._1, 0)
      case "SUM" => (a._1+b._1, 0)
      case "MIN" => (math.min(a._1, b._1), 0)
      case "MAX" => (math.max(a._1, b._1), 0)
      case "AVG" => ((a._1*a._2 + b._1*b._2)/(a._2 + b._2), a._2 + b._2)
    })
    // compute partial values
    .flatMap { p =>
      val index = p._1.split(",")
      val permutations = math.pow(2, index.size).toInt - 1
      val out = new Array[(String, (Double, Int))](permutations)
      for (x <- 1 to permutations) {
        var partial_key: String = ""
        for (i <- index.indices) {
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

    //**** PHASE 2 ****
    partials.reduceByKey((a, b) => agg match {
      case "COUNT" => (a._1+b._1, 0)
      case "SUM" => (a._1+b._1, 0)
      case "MIN" => (math.min(a._1, b._1), 0)
      case "MAX" => (math.max(a._1, b._1), 0)
      case "AVG" => ((a._1*a._2 + b._1*b._2)/(a._2 + b._2), a._2 + b._2)
    })
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
    val out = new Array[(String, (Double,Int))](permutations)
    for (x <- 1 to permutations) {
      var partial_key:String = ""
      for (i <- index.indices) {
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
    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    val cube = rdd.map(r => format(r, index, indexAgg, agg))
      // combine locally, shuffle, and reduce
      .reduceByKey((a, b) => mergeValues(a, b, agg))
      // strip counts
      .map(p => (p._1, p._2._1))

    val permutations = math.pow(2, index.size).toInt - 1
    for (x <- 1 to permutations) {
      var subset = ArrayBuffer.empty[Int]
      for (i <- index.indices) {
        if ((x / math.pow(2, i) % 1) == 0) {
          subset.add(i)
        }
      }

      // Map
      val partial = rdd.map(r => format(r, subset.toList, indexAgg, agg))
        // combine locally, shuffle, and reduce
        .reduceByKey((a, b) => mergeValues(a, b, agg))
        // strip counts
        .map(p => (p._1, p._2._1))

      cube.union(partial).collect
    }

    cube
  }

}
