create database analytics;
use analytics;

select * from messages;



-- MySQL Script generated by MySQL Workbench
-- Thu Jul 14 17:10:40 2022
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema analytics
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema analytics
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `analytics` DEFAULT CHARACTER SET utf8 ;
USE `analytics` ;

-- -----------------------------------------------------
-- Table `analytics`.`messages`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `analytics`.`messages` (
  `name` VARCHAR(300) NULL,
  `type` VARCHAR(45) NULL,
  `id` VARCHAR(45) NOT NULL,
  `timestamp` TIMESTAMP NULL,
  `elapsedTime` TIMESTAMP NULL,
  `message` VARCHAR(500) NULL,
  `amountValue` FLOAT NULL,
  `amountString` VARCHAR(45) NULL,
  `channelId` VARCHAR(45) NULL,
  `bgColor` VARCHAR(45) NULL,
  `topic` VARCHAR(45) NULL,
  `badgeUrl` VARCHAR(500) NULL,
  `imageUrl` VARCHAR(500) NULL,
  `isChatModerator` TINYINT NULL,
  `isChatOwner` TINYINT NULL,
  `isChatSponsor` TINYINT NULL,
  `isVeriefied` TINYINT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `mydb`.`author`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `analytics`.`author` (
  `name` VARCHAR(500) NULL,
  `type` VARCHAR(500) NULL,
  `isVerified` BOOLEAN NULL,
  `isChatOwner` BOOLEAN NULL,
  `isChatSponsor` BOOLEAN NULL,
  `channelUrl` VARCHAR(500) NULL,
  `imageUrl` VARCHAR(500) NULL,
  `channelId` VARCHAR(500) NOT NULL)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `mydb`.`message`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `analytics`.`message` (
  `type` VARCHAR(45) NULL,
  `timestamp` BIGINT NULL,
  `elapsedTime` VARCHAR(500) NULL,
  `datetime` DATETIME NULL,
  `message` TEXT NULL,
  `amountValue` FLOAT NULL,
  `amountString` VARCHAR(45) NULL,
  `currency` VARCHAR(500) NULL,
  `bgColor` BIGINT NULL,
  `topic` VARCHAR(45) NULL,
  `id` VARCHAR(500) NOT NULL,
  `channelId` VARCHAR(500) NOT NULL)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;