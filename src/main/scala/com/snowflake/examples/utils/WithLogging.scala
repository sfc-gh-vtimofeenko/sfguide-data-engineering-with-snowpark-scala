package com.snowflake.examples.utils

import org.slf4j.LoggerFactory

trait WithLogging {
  val logger = LoggerFactory.getLogger(getClass.getName)
}
