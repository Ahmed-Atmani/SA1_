package Common

import scala.collection.mutable.Map as MutMap


/**
 * Keeps a counter for multiple strings
 * @param map a mutable map to be used as an initial map
 * @param printFunc a function that returns a string representation of a map
 */
class MultiCounter(val map: MutMap[String, Int], val printFunc: MutMap[String, Int] => String):

  /**
   * Alternative constructor that takes an initial team name and count instead of a map
   * @param team_name
   * @param count
   * @param printFunc
   */
  def this(team_name: String, count: Int, printFunc: MutMap[String, Int] => String) = 
    this(MutMap((team_name, count)), printFunc)

  /**
   * Overloaded + operator for simple reduce/fold operations on streams
   * @param that other MultiCounter object
   * @return
   */
  def +(that: MultiCounter): MultiCounter =
    val temp: MutMap[String, Int] = this.map.clone()
    for key <- that.map.keys do
      val thatCount: Int = that.map(key)
      temp.get(key) match
        case Some(thisCount) => temp(key) = thatCount + thisCount
        case None => temp.addOne(key, thatCount)
    new MultiCounter(temp, printFunc)

  override def toString: String = printFunc(this.map)


/**
 * Keeps a counter for a single string
 * @param name string (or name of a team)
 * @param counter intitial count
 */
class SingleCounter(val name: String, val counter: Int = 1):
  def +(that: SingleCounter): SingleCounter = SingleCounter(that.name, this.counter + that.counter)
  override def toString: String = s"$name - $counter"

