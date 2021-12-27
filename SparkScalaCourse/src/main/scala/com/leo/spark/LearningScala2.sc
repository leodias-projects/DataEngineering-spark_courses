// Flow control

if (1 > 3) println("Impossible!") else println("The world makes sense")

if (1 > 3){
  print("Impossible")
}
else{
  println("The world makes sense")
}

// Matching

val number = 40
number match {
  case 1 => println("One")
  case 2 => println("Two")
  case 3 => println("Third")
  case _ => println("Something else")
}

// for loops

for (x <- 1 to 4){
  val squared = x * x
  println(squared)
}

// while
var x = 10
while (x >= 0){
  println(x)
  x -= 1
}

// do while

x = 0
do { println(x); x+=1 } while (x <= 10)

// Expressions

{val x = 10; x + 20}

println({val x = 10; x + 20})

// EXERCISE
// Write some code that prints out the first 10 values of the Fibonacci sequence.
// This is the sequence where every number is the sum of the two numbers before it.
// So, the result should be 0, 1, 1, 2, 3, 5, 8, 13, 21, 34

def fib3(n: Int): Int = {
  def fib_tail(n: Int, a: Int, b: Int): Int = n match {
    case 0 => a
    case _ => fib_tail(n - 1, b, a + b)
  }
  return fib_tail(n, 0 , 1)
}

for (x <- 1 to 10){
  println(fib3(x))
}