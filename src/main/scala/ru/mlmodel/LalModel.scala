package ru.mlmodel

import ai.catboost.spark.{CatBoostClassificationModel, CatBoostClassifier, Pool}
import breeze.linalg.min
import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, count, lag, round, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mlflow.tracking.MlflowClient
import ru.model.ModelComponents

class LalModel(spark: SparkSession, mlflowClient: MlflowClient, mlflowRunId: String) {
  import spark.implicits._
  val logger: Logger = Logger("LAL feature extractor Logger")

  def trainAndValidate(mainDf: DataFrame): ModelComponents={
    val leakThreshold =  0.95
    var modelComponents:ModelComponents = trainModel(mainDf)
    if (modelComponents.score > leakThreshold ){
      val leakFeatureName = modelComponents.model
        .getFeatureImportancePrettified()
        .head
        .featureName
      modelComponents = trainModel(mainDf, leakFeatureName)
    }
    mlflowClient.logMetric(mlflowRunId, "accuracy", modelComponents.score)

    modelComponents
  }
  private def trainModel(mainDf: DataFrame, filterFeature:String = "null"): ModelComponents ={
    val assembler = new VectorAssembler()
      .setInputCols(mainDf
        .columns
        .filter(_!=filterFeature)
        .filter(_!="label")
        .filter(_!="interests")
        .filter(_!="gid")
        .filter(_!="machash")
        .filter(_!="mac"))
      .setOutputCol("features")

    val assembledDf = assembler.transform(mainDf)

    val trainSample = 0.5
    val Array(trainingDf, testDf) = assembledDf
      .sample(trainSample)
      .randomSplit(Array[Double](0.8, 0.2), 42)

    val trainPool = new Pool(trainingDf)
    val evalPool = new Pool(testDf)
    val classifier = new CatBoostClassifier()
    val model = classifier.fit(trainPool, Array[Pool](evalPool))

    val predictions: DataFrame = model.transform(evalPool.data)

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")

    ModelComponents(model, assembler, evaluator.evaluate(predictions))
  }

  def getPrediction(modelComponents: ModelComponents, predictionDf: DataFrame, predictionSize: Int): DataFrame = {
    val getProbability = udf { (v: Vector) =>
      v(1)
    }

    val assembledDf = modelComponents.assembler.transform(predictionDf)
    val predictionPool = new Pool(assembledDf)

    val predictedSegment = modelComponents.model
      .transform(predictionPool.data)
      .withColumn("segmentProbability", round(getProbability($"probability"), 2))
      .select("mac", "segmentProbability")
      .repartition($"segmentProbability")

    predictedSegment.persist()

    val cumsumWindow = Window
      .orderBy($"segmentProbability".desc)
    logger.info(f"predictionSize is $predictionSize")

    val testSize = predictedSegment.count()
    logger.info(f"testSize is $testSize")

    val tmp = predictedSegment.groupBy($"segmentProbability".alias("segmentProbability"))
      .agg(count("mac").alias("groupSize"))
      .withColumn("cumSum", sum($"groupSize").over(cumsumWindow))
      .withColumn("lagCumSum", lag($"cumSum", 1).over(cumsumWindow))
      .filter( $"cumSum" >= min(Seq(predictionSize, testSize)))
      .orderBy($"segmentProbability".desc)

    tmp.persist()

    val threshProb = tmp
      .select("segmentProbability")
      .head()
      .getDouble(0)

    val lagCumSum = tmp
      .select("lagCumSum")
      .head()
      .getLong(0)

    tmp.unpersist()

    val extendedSegment = predictedSegment
      .filter($"segmentProbability" > threshProb)
      .union( predictedSegment
        .filter($"segmentProbability" === threshProb)
        .limit((predictionSize.longValue()-lagCumSum).toInt))

    predictedSegment.unpersist()
    extendedSegment.cache()
   val avgProb = extendedSegment
      .select(avg($"segmentProbability"))
      .first.getDouble(0)

    mlflowClient.logMetric(mlflowRunId, "avgProb", avgProb)

    extendedSegment
  }
}
