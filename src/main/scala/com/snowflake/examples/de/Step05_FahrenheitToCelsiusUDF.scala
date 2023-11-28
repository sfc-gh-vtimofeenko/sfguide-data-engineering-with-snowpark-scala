package com.snowflake.examples.de

class Step05_FahrenheitToCelsiusUDF {

  /** Converts Fahrenheit to Celsius */
  def fahrenheitToCelsius(fahrenheit: Double): Double = (fahrenheit - 32) * 5 / 9

  def main(args: Array[String]): Unit = {
    val fahrenheit: Double = 98.6
    val celsius = fahrenheitToCelsius(fahrenheit)
    println(f"$fahrenheit%.2f°F is equal to $celsius%.2f°C")
  }

}
