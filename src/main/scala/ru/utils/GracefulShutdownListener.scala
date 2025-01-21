package ru.utils

import com.typesafe.scalalogging.Logger
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import ru.featurepreparation.SegmentatorApi
import ru.model.{LalResult, LalSegment}

class GracefulShutdownListener (segment: LalSegment, segmentator: SegmentatorApi) extends SparkListener {
  val logger: Logger = Logger("Lal GracefulShutdown Logger")
  val buildingStatus = "BUILDING"
  val failedStatus = "FAILED"

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      if (segmentator.checkStatus(segment, buildingStatus)) {
        logger.info("Graceful SparkContext shutdown started..")
        segmentator.changeSegmentStatus(segment, LalResult(status = failedStatus, error = Some("SparkContext shutdown")))
        logger.info("Graceful SparkContext shutdown completed")
      }
    }
  }
