package ru.configs

/**
 * Конфигурация для подключения к Mlflow
 *
 * @param mlflowTrackingUrl   url к Mlflow
 * @param experimentId      Имя эксперимента в Mlflow
 */
case class MlflowConfig(mlflowTrackingUrl: String,
                        experimentId: Int)
