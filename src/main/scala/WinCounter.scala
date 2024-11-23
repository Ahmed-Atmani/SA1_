import scala.collection.mutable.Map as MutMap

class WinCounter(val map: MutMap[String, Int]):
  def this(team_name: String) = this(MutMap((team_name, 1)))

  def +(that: WinCounter): WinCounter =
    val temp: MutMap[String, Int] = this.map.clone()
    for key <- that.map.keys do
      val thatCount: Int = that.map(key)
      temp.get(key) match
        case Some(thisCount) => temp(key) = thatCount + thisCount
        case None => temp.addOne(key, thatCount)
    new WinCounter(temp)

  override def toString: String =
    var str: String = ""
    this.map.foreach((team, win_count) => str += s"Name: $team --> Won Games on Sundays: $win_count\n")
    str

//  object WinCounter:
//    val emptyWinCounter: WinCounter = new WinCounter(MutMap[String, Int]())