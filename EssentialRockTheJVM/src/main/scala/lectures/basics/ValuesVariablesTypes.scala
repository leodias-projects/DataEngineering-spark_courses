package lectures.basics

object ValuesVariablesTypes extends App {

  val x = 42
  println(x)

  // VALS ARE IMMUTABLE
  // Type inference

  val aString: String = "Hello"; val anotherString = "goodBye" // discouraged
  val moreOne = "Encouraged"

  val aBoolean: Boolean = true // or false
  val aChar: Char = 'a' // single quotes
  val anInt: Int = x
  val aShort: Short = 4613
  val aLong: Long = 4353490523243L
  val aFloat: Float = 2.0f
  val aDouble: Double = 3.14

  // Variables

  var aVariable: Int = 4

  aVariable = 5 // Variables are used for side effects (functional programming term)
}
