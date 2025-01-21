package ru.configs.exceptions

/**
 * Ошибка парсинга запроса
 *
 * @param msg текст сообщения
 */
class ParsingException(msg: String) extends Exception(s"Failed to parse request: $msg")
