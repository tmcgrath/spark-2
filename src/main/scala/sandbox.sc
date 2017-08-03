
val n = List(1, 2, 3, 4)
n.map((i: Int) => i * 2)


class Cow {
  private var age  = 0
  Cow.countMe()
}

object Cow {
  private var numCows = 0
  private def countMe() = numCows += 1
  def getNumCows = numCows
}

//val betsy = new Cow
assert(Cow.getNumCows == 1)