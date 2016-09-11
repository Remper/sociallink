CREATE DATABASE `alignments`;

USE `alignments`;

CREATE TABLE `alignments` (
  `resource_id` varchar(255) NOT NULL,
  `twitter_id` bigint(20) NOT NULL,
  `score` varchar(255) NOT NULL,
  `is_alignment` bool NOT NULL,
  PRIMARY KEY (`resource_id`, `twitter_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `profiles` (
  `twitter_id` bigint(20) NOT NULL,
  `username` varchar(255) NOT NULL,
  PRIMARY KEY (`twitter_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;