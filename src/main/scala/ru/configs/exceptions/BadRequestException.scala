package ru.configs.exceptions

/**
 * Ошибка для неуспешного запроса
 *
 * @param msg текст сообщения
 */
class BadRequestException(msg: String) extends Exception(s"Failed to get response: $msg")
