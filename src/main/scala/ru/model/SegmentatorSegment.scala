package ru.model


case class SegmentatorSegment(segId: String,
                              dmpId: Int,
                              orgId: String,
                              status: String,
                              dmpName: String,
                              `type`: String,
                              externalId: String,
                              serviceId: String,
                              hitCount: Int,
                              lastRun: String,
                              idsTotalCount: Int,
                              idsUploadedCount: Int)
