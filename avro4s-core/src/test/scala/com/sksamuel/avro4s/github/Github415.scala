//package com.sksamuel.avro4s.github
//
//import java.io.{FileOutputStream, ObjectOutputStream}
//
//import com.sksamuel.avro4s.Encoder
//import com.sksamuel.avro4s.github.Github415.PlaybackSession
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.must.Matchers
//
//class Github415 extends AnyFunSuite with Matchers {
//
//  test("github 415") {
//    val fileOut = new FileOutputStream("remove_me")
//    val out = new ObjectOutputStream(fileOut)
//    out.writeObject(Encoder[PlaybackSession])
//  }
//}
//
//object Github415 {
//  object Rebuffers {
//    case class Metrics(count: Int)
//    case class EarlyLate(early: Metrics)
//    case class Stats(session: Option[EarlyLate])
//  }
//
//  case class Rebuffers(network: Option[Rebuffers.Stats])
//
//  case class PlaybackSession(rebuffers: Option[Rebuffers])
//}