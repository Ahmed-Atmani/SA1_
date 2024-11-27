import scala.collection.mutable.Map as MutMap


class MultiCounter(val map: MutMap[String, Int], val printFunc: MutMap[String, Int] => String):
  def this(team_name: String, count: Int, printFunc: MutMap[String, Int] => String) = 
    this(MutMap((team_name, count)), printFunc)

  def +(that: MultiCounter): MultiCounter =
    val temp: MutMap[String, Int] = this.map.clone()
    for key <- that.map.keys do
      val thatCount: Int = that.map(key)
      temp.get(key) match
        case Some(thisCount) => temp(key) = thatCount + thisCount
        case None => temp.addOne(key, thatCount)
    new MultiCounter(temp, printFunc)

  override def toString: String = printFunc(this.map)

class SingleCounter(val name: String, val counter: Int):
  def +(that: SingleCounter): SingleCounter = SingleCounter(that.name, this.counter + that.counter)
  override def toString: String = s"$name - $counter"
