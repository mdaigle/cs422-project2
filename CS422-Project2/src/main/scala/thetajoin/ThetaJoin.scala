package thetajoin

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

class ThetaJoin(numR: Long, numS: Long, reducers: Int, bucketsize: Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("ThetaJoin")    
  
  // random samples for each relation
  // helper structures, you are allowed
  // not to use them
  var horizontalBoundaries = Array[Int]()
  var verticalBoundaries = Array[Int]()
  
  // number of values that fall in each partition
  // helper structures, you are allowed
  // not to use them
  var horizontalCounts = Array[Int]()
  var verticalCounts = Array[Int]()      

  class Cell(verticalIndex:Int, horizontalIndex:Int, isCandidateCell:Boolean) {
    def hIndex():Int=horizontalIndex
    def vIndex():Int=verticalIndex
    def isCandidate():Boolean=isCandidateCell

    def contains(x: Int, fromR: Boolean): Boolean = {
      var boundaries = verticalBoundaries
      var index = verticalIndex
      if (!fromR) {
        boundaries = horizontalBoundaries
        index = horizontalIndex
      }

      if (index == 0 && x < boundaries(0)) true
      if (index == boundaries.length && x > boundaries.last) true
      if (boundaries(index - 1) <= x && x < boundaries(index)) true

      false
    }
  }

  class Region() {
    var num_cells = 0
    var cells = Array[Cell]()
    var cap = bucketsize

    def addCell(c: Cell) = {
      cells(num_cells) = c
      num_cells += 1
      if(c.isCandidate()) cap = cap-1
    }

    def addCells(cells: Array[Cell]) = {
      for (cell <- cells) {
        this.addCell(cell)
      }
    }

    def contains(x: Int, fromR: Boolean): Boolean = {
      for (cell <- cells) {
        if (cell.contains(x, fromR)) true
      }
      false
    }

    def capacity(): Int = cap

    def size(): Int = num_cells

    def getRS(): Int = {
      var min = cells(0).vIndex()
      for (c <- cells) {
        if (c.vIndex() < min) min = c.vIndex()
      }
      min
    }

    def getRE(): Int = {
      var max = cells(0).vIndex()
      for (c <- cells) {
        if (c.vIndex() > max) max = c.vIndex()
      }
      max
    }
    def getCS(): Int = {
      var min = cells(0).hIndex()
      for (c <- cells) {
        if (c.hIndex() < min) min = c.hIndex()
      }
      min
    }
    def getCE(): Int = {
      var max = cells(0).hIndex()
      for (c <- cells) {
        if (c.hIndex() > max) max = c.hIndex()
      }
      max
    }
  }

  /*
   * this method gets as input two datasets and the condition
   * and returns an RDD with the result by projecting only 
   * attr1 and attr2
   * You are not allowed to change the definition of this function.
   * */  
  def theta_join(dataset1: Dataset, dataset2: Dataset, attr1:String, attr2:String, op:String): RDD[(Int, Int)] = {
    val schema1 = dataset1.getSchema
    val schema2 = dataset2.getSchema        
    
    val rdd1 = dataset1.getRDD
    val rdd2 = dataset2.getRDD
    
    val index1 = schema1.indexOf(attr1)
    val index2 = schema2.indexOf(attr2)        

    // TODO: int division?
    val cr = numR / math.sqrt(numR * numS / reducers)
    val cs = numS / math.sqrt(numR * numS / reducers)

    // Sample boundaries
    verticalBoundaries = rdd1.sample(false, cr / numR).map(r => r.getInt(index1)).collect().sorted
    horizontalBoundaries = rdd2.sample(false, cs / numS).map(r => r.getInt(index2)).collect().sorted

    // Compute R histogram
    val r_counts = rdd1.map { r =>
        val r_val = r.getInt(index1)
        val value = 1
        var key = verticalBoundaries.length
        for (i <- 1 to verticalBoundaries.length) {
          if (r_val < verticalBoundaries(i)) {
            key = i
          }
        }

        (key, value)
      }
      .reduceByKey((a, b) =>  a + b).collect()

    for (i <- 0 to verticalBoundaries.length) verticalCounts(i) = 0
    for ((k,v) <- r_counts) verticalCounts(k) = v

    // Compute S histogram
    val s_counts = rdd2.map { s =>
      val s_val = s.getInt(index2)
      val value = 1
      var key = horizontalBoundaries.length
      for (i <- 1 to horizontalBoundaries.length) {
        if (s_val < horizontalBoundaries(i)) {
          key = i
        }
      }

      (key, value)
    }
    .reduceByKey((a, b) =>  a + b).collect()

    for (i <- 0 to horizontalBoundaries.length + 1) horizontalCounts(i) = 0
    for ((k,v) <- s_counts) horizontalCounts(k) = v

    // Compute regions using M-Bucket-I
    val best_regions = mBucketI()

    if (best_regions == null) {
      throw new RuntimeException("Insufficient reducers, or maxInput too low")
    }

    // Map rows to regions
    val r_regions = rdd1.map(row => row.getInt(index1))
      .map { value =>
        var ri = 0
        for (i <- best_regions.indices) {
          if (best_regions(i).contains(value, true)) {ri = i}
        }
        (ri, value)
      }
    var s_regions = rdd2.map(row => row.getInt(index2))
      .map { value =>
        var ri = 0
        for (i <- best_regions.indices) {
          if (best_regions(i).contains(value, true)) {ri = i}
        }
        (ri, value)
      }

    // Partition by region
    val r_partitioned = r_regions.groupByKey(reducers)
    val s_partitioned = s_regions.groupByKey(reducers)

    // Locally theta-join each partition
    val groups = r_partitioned.join(s_partitioned)
    groups.flatMap{g =>
      local_thetajoin(g._2._1.iterator, g._2._2.iterator, op)
    }
  }

  def mBucketI() : Array[Region] = {
    var row = 0
    var r = reducers

    var regions = Array[Region]()

    while (row < verticalCounts.length) {
      val out =  coverSubMatrix(row, r)
      row = out._1
      r = out._2
      // Union regions
      regions = regions ++ out._3

      if (r < 0) return null
    }

    regions
  }

  def coverSubMatrix(row:Int, r:Int) : (Int, Int, Array[Region]) = {
    var max_score = -1
    var r_used = 0
    var best_row = row + 1
    var best_regions = Array[Region]()

    var i = 1
    while (i < bucketsize && row + i < verticalCounts.length) {
      val ri = coverRows(row, row + i)
      val area = totalCandidateArea(row, row+i)
      val score = area / ri.length

      if (score >= max_score) {
        max_score = score
        best_regions = ri
        best_row = row+i
        r_used = ri.length
      }

      i += 1
    }

    val r_remaining = r - r_used
    (best_row+1, r_remaining, best_regions)
  }

  def coverRows(first:Int, last:Int) : Array[Region] = {
    var regions = List[Region]()
    var r = new Region()

    for (col <- horizontalCounts.indices) {
      var cells = Array[Cell]()
      for (row <- first to last) {
        cells = cells ++ Array{new Cell(row, col, isCandidate(row, col))}
      }

      val numCandidates = cells.count(cell => cell.isCandidate())

      if (r.capacity() < numCandidates) {
        regions = regions :+ r
        r = new Region()
      }

      r.addCells(cells)
    }

    regions.toArray
  }

  /**
    *      0  1  2  3
    *    __ __ __ __ __
    *   |__|__|__|__|__|
    * 0 |__|_-|_-|_-|__|
    * 1 |__|_-|_-|_-|__|
    * 2 |__|_-|_-|_-|__|
    *
    */
  def isCandidate(row: Int, col: Int): Boolean = {
    // Edge Cases
    if (row == 0 && col == 0) true
    if (row == 0) verticalBoundaries(0) > horizontalBoundaries(col - 1)
    if (col == 0) horizontalBoundaries(0) > verticalBoundaries(row - 1)
    if (row == verticalBoundaries.length && col == horizontalBoundaries.length) true
    if (row == verticalBoundaries.length) {
      verticalBoundaries(verticalBoundaries.length - 1) < horizontalBoundaries(col)
    }
    if (col == horizontalBoundaries.length) {
      horizontalBoundaries(horizontalBoundaries.length - 1) < verticalBoundaries(row)
    }

    // General Case
    val lowerR = verticalBoundaries(row - 1)
    val upperR = verticalBoundaries(row)

    val lowerC = horizontalBoundaries(col - 1)
    val upperC = horizontalBoundaries(col)

    (lowerC <= lowerR && lowerR < upperC) || (lowerC < upperR && upperR <= upperC)
  }

  def totalCandidateArea(first: Int, last: Int): Int = {
    var area = 0
    for (col <- horizontalCounts.indices) {
      for (row <- first to last) {
        if (isCandidate(row, col)) area += 1
      }
    }
    area
  }
    
  /*
   * this method takes as input two lists of values that belong to the same partition
   * and performs the theta join on them. Both datasets are lists of tuples (Int, Int)
   * where ._1 is the partition number and ._2 is the value. 
   * Of course you might change this function (both definition and body) if it does not 
   * fit your needs :)
   * */  
  def local_thetajoin(dat1:Iterator[Int], dat2:Iterator[Int], op:String) : Iterator[(Int, Int)] = {
    var res = List[(Int, Int)]()
    var dat2List = dat2.toList
        
    while(dat1.hasNext) {
      val row1 = dat1.next()      
      for(row2 <- dat2List) {
        if(checkCondition(row1, row2, op)) {
          res = res :+ (row1, row2)
        }        
      }      
    }    
    res.iterator
  }  
  
  def checkCondition(value1: Int, value2: Int, op:String): Boolean = {
    op match {
      case "=" => value1 == value2
      case "<" => value1 < value2
      case "<=" => value1 <= value2
      case ">" => value1 > value2
      case ">=" => value1 >= value2
    }
  }    
}

