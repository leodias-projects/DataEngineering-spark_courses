package lectures.basics

object Functions extends App {

  def aFunction(a: String, b: Int): String = {
    a + " " + b
  }

  println(aFunction("Hello", 3))

  def aParameterlessFunction(): Int = 42

  println(aParameterlessFunction)

  def aRepeatedFunction(aString: String, n: Int): String = {
    if (n == 1) aString
    else aString + aRepeatedFunction(aString, n-1)
  }

  println(aRepeatedFunction("Hello", 3))

  // WHEN YOU NEED LOOPS, USE RECURSION.

  def aFunctionWithSideEffects(aString: String): Unit = println(aString)

  def aBigFunction(n: Int): Int = {
    def aSmallerFunction(a: Int, b:Int): Int = a + b

    aSmallerFunction(n, n-1)
  }

  /*
    1. A greeting function (name, age) => "Hi, my name is $name and I am $age years old."
    2. Factorial function
    3. Fibonacci function
      f(1) = 1
      f(2) = 1
      f(3) = 2
      f(n) = f(n -1) + f(n -2)
    4. Tests if a number is prime
   */

  def greetingFunction(name: String, age: Int): String = "Hi, my name is" + name + "and I am" + age +"years old."

  def factorialFunction(n: Int): Int = {
    if (n <= 0) 1
    else n * (factorialFunction(n - 1))
  }

  def fibonnaciFunction(n: Int): Int = {
    if (n <= 2) 1
    else fibonnaciFunction(n -1) + fibonnaciFunction(n - 2)
  }

  def numberIsPrime(n: Int): Boolean = {
    def isPrimeUntil(t: Int): Boolean =
      if (t <= 1) true
      else n % t != 0 && isPrimeUntil(t-1)

    isPrimeUntil(n / 2)
  }

  println(factorialFunction(12))
  println(fibonnaciFunction(9))
  println(numberIsPrime(41))

}
