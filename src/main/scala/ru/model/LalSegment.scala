package ru.model

case class LalSegment(taskId: String,
                      dmpId: String,
                      multiplicationFactor: Int,
                      includeSource: Boolean,
                      status: String,
                      createdAt:String,
                      updatedAt:String)
