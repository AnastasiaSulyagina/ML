import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import gnu.trove.list.array.TLongArrayList
import gnu.trove.map.hash.TIntShortHashMap

import scala.io.Source

case class Matrix(users: Int, connections: Int) {
  val data = Array.fill(connections) {0}
  var offsets = Array.fill(users) {0}
  var ids = Array.fill(users) {0}
  var friendNumbers = Array.fill(users) {0}

  def indexOf(x: Int) = util.Arrays.binarySearch(ids, x)

  def sort() = {
    offsets = offsets.zip(ids).sortBy(_._2).unzip._1
    friendNumbers = friendNumbers.zip(ids).sortBy(_._2).unzip._1
    ids = ids.sorted
  }

  def sortAll() = {
    sort()
    for (i <- 0 until users) {
      util.Arrays.sort(data, offsets(i), offsets(i) + friendNumbers(i))
    }
  }
}
class Timer {
  var lastTime = System.currentTimeMillis()


  def checkpoint(checkPointName:String): Unit = {
    val currentTime = System.currentTimeMillis()
    println(checkPointName + " took: " + (currentTime - lastTime))
    lastTime = currentTime
  }
}

object Baseline {
  System.setProperty("file.encoding", "UTF-8")
  private var tm = System.currentTimeMillis()
  private var users = 0
  private var edges = 0
  private var coreEdges = 0
  private var coreUsers = 0
  private val lock = new Object

  private val LIMIT = 4000000
  val progress = new AtomicInteger(0)

  implicit def funToRunnable(fun: () => Unit) : Runnable = new Runnable() { def run() = fun() }

  def pack(x: Int, y: Int): Long = (x.toLong << 32) + y

  def unpack(long: Long): (Int, Int) = ((long >> 32).toInt, (long & 0xFFFFFFFF).toInt)


  def main(args: Array[String]): Unit = {
    println("Started")
    val path = "graph/"
    val (invert, matrix) = initFrom(path)
    fill(path, invert, matrix)
    val pool: ExecutorService = Executors.newFixedThreadPool(4)
    val res = new PrintWriter(new BufferedWriter(new FileWriter("myresult.txt"), 100000))

    for (i <- 0 until matrix.users) {
      val id = matrix.ids(i)
      if (id % 11 == 7) {
        pool.submit(() => {

          val secondFriends = new TLongArrayList(10000000)
          val predictedFriends = new TLongArrayList(10000000)

          secondFriends.resetQuick()

          for (j <- 0 until matrix.friendNumbers(i)) {
            val fr = matrix.data(matrix.offsets(i) + j)
            val k = invert.indexOf(fr)

            for (g <- 0 until invert.friendNumbers(k)) {
              val v = pack(invert.data(invert.offsets(k) + g),
                Metrics.weight(invert.friendNumbers(k)) + Metrics.adamicAdar(invert.friendNumbers(k)))
              secondFriends.add(v)
            }
          }

          secondFriends.sort()
          predictedFriends.resetQuick()

          var scoreSum: Long = -1
          var prevId = -1

          for (j <- 0 until secondFriends.size()) {
            val (currentId, score) = unpack(secondFriends.get(j))

            if (currentId == prevId) {
              scoreSum += pack(score, 0)
              predictedFriends.setQuick(predictedFriends.size() - 1, scoreSum)
            } else {
              scoreSum = pack(score, currentId)
              predictedFriends.add(scoreSum)
            }
            prevId = currentId
          }
          predictedFriends.sort()
          predictedFriends.reverse()
          val count = Math.round(matrix.friendNumbers(i) * LIMIT / coreEdges)

          lock.synchronized {
            var ind = 0
            for (j <- 0 until predictedFriends.size() if ind < count) {
              val friendId = unpack(predictedFriends.get(j))._2

              if (friendId != id && util.Arrays.binarySearch(
                matrix.data, matrix.offsets(i), matrix.offsets(i) + matrix.friendNumbers(i), friendId) < 0) {
                if (ind == 0) res.print(id)
                res.print(" " + friendId)
                ind += 1
              }
            }
            if (ind > 0) res.println()
            val p = progress.incrementAndGet()
            if (p % 5000 == 0) {
              println("Progress: " + p)
              res.flush()
            }
          }
        })
      }
    }
    pool.shutdown()
    pool.awaitTermination(1, TimeUnit.DAYS)
    res.close()
  }

    def initFrom(path: String): (Matrix, Matrix) = {

      val invertUsers = new TIntShortHashMap()
      val timer = new Timer()
      for (file <- new File(path).listFiles()) {
        for (s <- Source.fromFile(file).getLines().filter(s => s != null)) {
          val tokens = s.split("[ \t(),{}]+")
          val user = tokens.head.toInt
          users += 1
          if (user % 11 == 7) coreUsers += 1
          tokens.drop(1).zipWithIndex.filter(_._2 % 2 == 0).map(_._1.toInt).foreach(friend => {
            invertUsers.adjustOrPutValue(friend, 1, 1)
            edges += 1
            if (user % 11 == 7) coreEdges += 1
          })
        }
      }
      timer.checkpoint("Data reading")
      val invert = new Matrix(invertUsers.size(), edges)

      val iter = invertUsers.iterator()
      var shift = 0
      for (i <- 0 until invert.users) {
        iter.advance()
        invert.ids(i) = iter.key()
        invert.offsets(i) = shift
        shift += iter.value()
      }
      invertUsers.clear()
      timer.checkpoint("Data constructing")

      invert.sort()
      timer.checkpoint("Data sorting")

      val matrix = new Matrix(users, edges)
      (invert, matrix)
    }

  def fill(path: String, invert: Matrix, matrix: Matrix): Unit = {
    val timer = new Timer()
    var index = 0
    var offset = 0
    var id = -1

    for (file <- new File(path).listFiles()) {
      for (s <- Source.fromFile(file).getLines().filter(s => s != null)) {
        val tokens = s.split("[ \t(),{}]+")
        id = tokens.head.toInt
        matrix.ids(index) = id
        matrix.offsets(index) = offset
        index += 1
        tokens.drop(1).zipWithIndex.filter(_._2 % 2 == 0).map(_._1.toInt).foreach(friend => {
          val invIndex = invert.indexOf(friend)
          matrix.data(offset) = friend
          matrix.friendNumbers(index - 1) += 1
          offset += 1
          invert.data(invert.offsets(invIndex) + invert.friendNumbers(invIndex)) = id
          invert.friendNumbers(invIndex) += 1
        })
      }
    }
    matrix.sortAll()
    invert.sortAll()
    timer.checkpoint("Data fill")
  }
}