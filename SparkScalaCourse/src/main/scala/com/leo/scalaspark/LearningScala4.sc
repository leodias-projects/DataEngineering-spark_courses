// Data structures

//Tuples
//Immutable lists

val captainStuff = ("Picard", "Enterprise-D", "NCC-1781-D")
println(captainStuff)

// Refer to the individual fields with a ONE-BASED index

println(captainStuff._1)
println(captainStuff._2)
println(captainStuff._3)

val picardsShip = "Picard" -> "Enterprise-D"
println(picardsShip._2)

val aBunchOfStuff = ("Kirk", 1964, true)

// Lists
// Must be of same type

val shipList = List("Enterprise", "Defiant", "Voyager", "Deep Space Nine")

// Refer to the individual fields with a ZERO-BASED index

println(shipList(0))
println(shipList.head)
println(shipList.tail)

for (ship <- shipList)
{
  println(ship)
}

val backwardsShips = shipList.map((ship: String) => {ship.reverse})
for (ship <- backwardsShips) {println(ship)}

// reduce() to combine together all items in a collection
// using some function

val numberList = List(1, 2, 3, 4, 5)
val sum = numberList.reduce( (x: Int, y: Int) => x + y)

// Filters to remove stuff

val iHateFives = numberList.filter( (x: Int) => x != 5)

val iHateThrees = numberList.filter(_ != 3)

// Concatenate Lists

val moreNumbers = List(6, 7, 8)
val lotsOfNumbers = numberList ++ moreNumbers

// Reverse

val reversed = numberList.reverse
val sorted = reversed.sorted

// Distinct
val lotsOfDuplicates = numberList ++ numberList
val distinctValues = lotsOfDuplicates.distinct

// Max
val maxValue = numberList.max
// Sum
val total = numberList.sum
// Check occurrence
val hasThree = iHateThrees.contains(3)

// Maps

val shipMap = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D", "Sisko" -> "Deep Scape Nine")
println(shipMap("Kirk"))
println(shipMap.contains("Archer"))

val archerShip = util.Try(shipMap("Archer")) getOrElse "Unknown"
println(archerShip)

val numList = List.range(1,21)
for (x <- numList ){
  if (x % 3 == 0) {
    println(x)
  }
}

val divisibleByThree = numList.filter(x => x % 3 == 0)
println(divisibleByThree)