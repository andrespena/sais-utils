
DROP KEYSPACE logging;
CREATE KEYSPACE logging WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE logging;

DROP TABLE logs;
CREATE TABLE logs (
  key                    timeuuid PRIMARY KEY,
  logger_name            varchar,
  log_level              varchar,
  log_timestamp          timestamp,
  context_host_name      varchar,
  context_host_ip        varchar,
  event_current_url      varchar,
  context_app_name       varchar,
  context_app_start_time timestamp,
  context_ndc            varchar,
  context_thread         varchar,
  context_file           varchar,
  context_class          varchar,
  context_method         varchar,
  context_line           varchar,
  message_exists         boolean,
  message_class          varchar,
  message_rendered       varchar,
  throwable_exists       boolean,
  throwable_class        varchar,
  throwable_message      varchar,
  throwable_stacktrace   varchar,
);


