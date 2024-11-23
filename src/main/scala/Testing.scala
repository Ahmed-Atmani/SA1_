object Testing extends App:
  val c1 = WinCounter("G2")
  val c2 = WinCounter("C9")
  val c3 = WinCounter("G2")

  println(c1)
  println(c2)
  println(c3)
  println(c1 + c2 + c3)
