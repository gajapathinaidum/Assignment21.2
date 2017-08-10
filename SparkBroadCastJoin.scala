package com.spark.core.prctice

import scala.tools.nsc.transform.Flatten

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkBroadCastJoin extends App{

val conf =new SparkConf().setMaster("local").setAppName("SparkBroadCastJoin")
val sc=new SparkContext(conf)

// Fact table
val flights = sc.parallelize(List(
("SEA", "JFK", "DL", "418", "7:00"),
("SFO", "LAX", "AA", "1250", "7:05"),
("SFO", "JFK", "VX", "12", "7:05"),
("JFK", "LAX", "DL", "424", "7:10"),
("LAX", "SEA", "DL", "5737", "7:10")))
// Dimension table
val airports = sc.parallelize(List(
("JFK", "John F. Kennedy International Airport", "New York", "NY"),
("LAX", "Los Angeles International Airport", "Los Angeles", "CA"),
("SEA", "Seattle-Tacoma International Airport", "Seattle", "WA"),
("SFO", "San Francisco International Airport", "San Francisco", "CA")))
// Dimension table
val airlines = sc.parallelize(List(
("AA", "American Airlines"),
("DL", "Delta Airlines"),
("VX", "Virgin America")))
case class Flights()
val airportKeyMap=airports.map(f=>(f._1,f._3))
val airlineKeyMap=airlines.map(f=>(f._1,f._2))
val flightsMapAirports=flights.map(f=>(f._1,(f._2,f._3,f._4,f._5)))
//val flightsMapAirLines=flights.map(f=>(f._3,(f._1,f._2,f._3,f._4,f._5)))
val broadcastAirports = sc.broadcast(airportKeyMap)
val broadcastAirlines= sc.broadcast(airlineKeyMap)
val joinAirports=flightsMapAirports.join(broadcastAirports.value).map(f=>f._2)
joinAirports.foreach(println)
val rejoinAirports=joinAirports.map(f=>(f._1._1,f._1._2,f._1._3,f._1._4,f._2)).map(f=>(f._1,(f))).join(broadcastAirports.value).map(f=>f._2)
val joinAirlines=rejoinAirports.map(f=>(f._1._2,(f._1._5,f._2,f._1._3,f._1._4))).join(broadcastAirlines.value)
var finalResult=joinAirlines.map(f=>(f._2)).map(f=>(f._1._1,f._1._2,f._2,f._1._3,f._1._4)).collect
finalResult.foreach(println)

}