
object Metrics {
  def jaccard(commonFriends: Double, friendsCount1: Int, friendsCount2: Int): Double = {
    val union = friendsCount1 + friendsCount2 - commonFriends
    if (union == 0) 0.0 else commonFriends.toDouble / union.toDouble
  }

  def cosine(commonFriends: Double, friendsCount1: Int, friendsCount2: Int): Double =
    if (friendsCount1 == 0 && friendsCount2 == 0)
      0.0
    else
      commonFriends.toDouble / math.sqrt(friendsCount1 * friendsCount2)

  def adamicAdar(n: Int): Int =
    Math.round(if (n >= 2) 1.0 / Math.log(n.toDouble) else 0.0).toInt

  //team00
  def weight(n: Int): Int =
    Math.round(100.0 / Math.pow(n + 5, 1 / 3.0) - 8).toInt
}
