package ru

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSessionr
import org.mlflow.tracking.MlflowContext
import ru.configs.{DefaultConfigRepository, HadoopConfigs, SegmentatorConfig, SparkConfig}
import ru.featurepreparation.{LalFeatureExtractor, SegmentatorApi}
import ru.mlmodel.LalModel
import ru.model.{LalResult, LalSegment, ModelComponents}
import ru.utils.{GracefulShutdownListener, SparkJob}


/**
 *  Джоба запуска обучения модели
 */

object LalJob extends App with SparkJob{

  val logger: Logger = Logger("Lal Logger")
  override val jobName = "LalSegmentsExtending"
  override val sparkConfig: SparkConfig = DefaultConfigRepository.sparkConfig
  override val hadoopConfigs: HadoopConfigs = DefaultConfigRepository.hadoopConfigs
  val buildingStatus = "BUILDING"
  val successStatus = "SUCCESS"
  val failedStatus = "FAILED"
  val newStatus = "NEW"

  // Инициализация клиента Mlflow
  val mlflowTrackingUri = DefaultConfigRepository.mlflowConfig.mlflowTrackingUrl
  val mlflowExperimentId =  DefaultConfigRepository.mlflowConfig.experimentId
//  val mlflowClient = new MlflowContext(mlflowTrackingUri).getClient

  val spark: SparkSession = initSparkSession

  val segmentatorUrl: String = DefaultConfigRepository.segmentatorConfig.segmentatorUrl
  val segmentator = new SegmentatorApi(segmentatorUrl)
  segmentator.loadSegment("29291_lal_10")
/*  val lalFeatureExtractor = new LalFeatureExtractor(spark)

  val segmentsList: Seq[LalSegment] = segmentator.getNewSegments
  for (segment <-segmentsList) {
    if (segmentator.checkStatus(segment, newStatus) || segmentator.checkStatus(segment, failedStatus)) {
      val currentSegmentListener = new GracefulShutdownListener(segment, segmentator)
      spark.sparkContext.addSparkListener(currentSegmentListener)
      segmentator.changeSegmentStatus(segment, LalResult(buildingStatus))
      val mlflowRunId = mlflowClient.createRun(mlflowExperimentId.toString).getRunId
      val dmpSegmentId = segment.dmpId
      mlflowClient.setTag(mlflowRunId, "DmpSegment", dmpSegmentId)
      val newDmpId = getLalAndWrite(dmpSegmentId, segment.multiplicationFactor, segment.includeSource, mlflowRunId)
      mlflowClient.setTag(mlflowRunId, "LalSegment", newDmpId.toString)
      segmentator.changeSegmentStatus(segment, LalResult(successStatus, Some(newDmpId)))
      logger.info(s"segment with task_id=${segment.taskId} loaded to Segmentator")
      spark.sparkContext.removeSparkListener(currentSegmentListener)
    }
  }

  def getLalAndWrite(segment: String, multiplicationFactor: Int, includeSource: Boolean, mlflowRunId: String): Int = {
    val lalModel = new LalModel(spark, mlflowClient, mlflowRunId)
    val (preparedData, dmpSegmentCount) = lalFeatureExtractor.getLalDf(segment)
    preparedData.trainDf.persist()
    preparedData.predictionDf.persist()
    preparedData.oldSegment.persist()
    val modelComponents: ModelComponents = lalModel.trainAndValidate(preparedData.trainDf)
    val multiplicationThreshold = (dmpSegmentCount * multiplicationFactor).toInt
    val newSegment = lalModel.getPrediction(modelComponents,preparedData.predictionDf, multiplicationThreshold)
    val newSegmentName = segment + s"_lal_$multiplicationFactor"
    if (includeSource) {
      segmentator.writeSegment(newSegment.select("mac").union(preparedData.oldSegment.select("mac")), newSegmentName)}
    else {
      segmentator.writeSegment(newSegment, newSegmentName)
    }
    segmentator.loadSegment(newSegmentName)
  }


 */
  spark.stop()
}
