package ru.model

case class LalResult(status: String, lalDmpId: Option[Int] = None, error: Option[String] = None)
