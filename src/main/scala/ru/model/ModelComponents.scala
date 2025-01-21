package ru.model

import ai.catboost.spark.CatBoostClassificationModel
import org.apache.spark.ml.feature.VectorAssembler

case class ModelComponents(model: CatBoostClassificationModel, assembler: VectorAssembler, score: Double)
