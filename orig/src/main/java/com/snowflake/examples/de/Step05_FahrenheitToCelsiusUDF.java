package com.snowflake.examples.de;

public class Step05_FahrenheitToCelsiusUDF {

    /**
     * Converts Fahrenheit to Celsius.
     *
     * @param fahrenheit Temperature in Fahrenheit
     * @return Temperature in Celsius
     */
    public static double fahrenheitToCelsius(double fahrenheit) {
        return (fahrenheit - 32) * 5 / 9;
    }

    public static void main(String[] args) {
        double fahrenheit = 98.6;
        double celsius = fahrenheitToCelsius(fahrenheit);

        System.out.printf("%.2f°F is equal to %.2f°C%n", fahrenheit, celsius);
    }
}
