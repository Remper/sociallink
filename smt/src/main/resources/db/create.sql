CREATE DATABASE `alignments`;

USE `alignments`;

CREATE TABLE `alignments` (
  `resource_id` varchar(255) NOT NULL,
  `twitter_id` bigint(20) NOT NULL,
  `score` varchar(255) NOT NULL,
  `is_alignment` bool NOT NULL,
  PRIMARY KEY (`resource_id`, `twitter_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `resources` (
  `resource_id` varchar(255) NOT NULL,
  `is_dead` bool NOT NULL,
  `dataset` varchar(255) NOT NULL DEFAULT 'generic',
  KEY (`dataset`),
  PRIMARY KEY (`resource_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `profiles` (
  `twitter_id` bigint(20) NOT NULL,
  `username` varchar(255) NOT NULL,
  UNIQUE KEY (`username`),
  PRIMARY KEY (`twitter_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;