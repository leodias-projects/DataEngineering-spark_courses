package lectures.basics

object Expressions extends App{

  val x = 1 + 2 // Expression
  println(x)

  println(2 + 3 * 4)
  // + - * / & | ^ << >> >>> (Right shift with 0 extension)

  println(1 == x)
  // == != > >= < <=

  println(!(1 == x))
  // ! && ||

  var aVariable = 2
  aVariable += 3 // also works with -= *= /= (only works with variables, all side effects)
  println(aVariable)

  // Instructions (DO) vs Expressions (VALUE)

  // IF expression

  val aCondition = true
  val aConditionValue = if(aCondition) 5 else 3
  println(aConditionValue)
  println(if(aCondition) 5 else 3)

  // Scala discourage loops

  var i = 0
  val aWhile = while (i < 10){
    println(i)
    i += 1
  }

  // NEVER WRITE THIS AGAIN
  // EVERYTHING IN SCALA IS AN EXPRESSION

  val aWeirdValue = (aVariable = 3) // Unit == void. Side effects are expressions returning unit
  println(aWeirdValue)

  // side effects: println, whiles, reassign

  // Code blocks

  val aCodeBlock = {
    val y = 2
    val z = y + 1
    if (z > 2) "hello" else "goodbye"
  }
  // Value of the block is the value of the last expression
  // values/variables inside the code block is invisible to outside

  // 1. difference between "hello world" vs println("hello world")?
  // "hello world" returns string, println("hello world") returns unit()

  // 2.

  val someValue = {
    2 < 3
  } // boolean false

  val someOtherValue = {
    if(someValue) 239 else 986
    42
  }
}
