package ru.configs.exceptions

class BadLalException(segment: String, msg: String) extends Exception(s"Failed to get lal for task_id=$segment: $msg")

