package ru.configs.exceptions

/**
 * Ошибка разбора конфигурации
 *
 * @param msg текст сообщения
 */
class ConfigParseException(msg: String) extends Exception(s"Failed to parse config: $msg")
