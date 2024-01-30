-- MySQL Script generated by MySQL Workbench
-- Wed Jan 24 15:08:33 2024
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema pms_db
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `pms_db` ;

-- -----------------------------------------------------
-- Schema pms_db
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `pms_db` DEFAULT CHARACTER SET utf8 ;
USE `pms_db` ;

-- -----------------------------------------------------
-- Table `pms_db`.`CUSTOMERcontact`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`CUSTOMERcontact` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`CUSTOMERcontact` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `firstName` VARCHAR(64) NULL DEFAULT '',
  `lastName` VARCHAR(64) NULL DEFAULT '',
  `title` VARCHAR(32) NULL DEFAULT '',
  `email` VARCHAR(64) NULL DEFAULT '',
  `birthDate` DATE NULL,
  `languageCode` VARCHAR(16) NULL DEFAULT '',
  `languageFormat` VARCHAR(32) NULL DEFAULT '',
  `extGuestId` VARCHAR(45) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONlibRoom`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONlibRoom` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONlibRoom` (
  `id` BIGINT UNSIGNED NOT NULL,
  `status` TINYINT NULL DEFAULT 0,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `roomNumber` VARCHAR(32) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONlibSource`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONlibSource` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONlibSource` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 0,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `sourceName` VARCHAR(64) NULL DEFAULT '',
  `sourceType` VARCHAR(32) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONlibProperty`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONlibProperty` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONlibProperty` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 0,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `chainCode` VARCHAR(32) NULL DEFAULT '',
  `propertyCode` VARCHAR(32) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONstay`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONstay` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONstay` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 0,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `createDateTime` INT NULL,
  `modifyDateTime` INT NULL,
  `startDate` DATE NULL,
  `endDate` DATE NULL,
  `createdBy` VARCHAR(64) NULL DEFAULT '',
  `metaData` JSON NULL,
  `extPMSConfNum` VARCHAR(45) NULL DEFAULT '',
  `extGuestId` VARCHAR(45) NULL DEFAULT '',
  `dataSource` VARCHAR(24) NULL,
  `libSourceId` BIGINT UNSIGNED NOT NULL,
  `libPropertyId` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_RESERVATIONstay_RESERVATIONlibsource1`
    FOREIGN KEY (`libSourceId`)
    REFERENCES `pms_db`.`RESERVATIONlibSource` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_RESERVATIONstay_RESERVATIONlibproperty1`
    FOREIGN KEY (`libPropertyId`)
    REFERENCES `pms_db`.`RESERVATIONlibProperty` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_RESERVATIONstay_RESERVATIONlibsource1_idx` ON `pms_db`.`RESERVATIONstay` (`libSourceId` ASC) VISIBLE;

CREATE INDEX `fk_RESERVATIONstay_RESERVATIONlibproperty1_idx` ON `pms_db`.`RESERVATIONstay` (`libPropertyId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `pms_db`.`CUSTOMERlibLoyaltyProgram`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`CUSTOMERlibLoyaltyProgram` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`CUSTOMERlibLoyaltyProgram` (
  `id` BIGINT UNSIGNED NOT NULL,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `name` VARCHAR(64) NULL DEFAULT '',
  `source` VARCHAR(64) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`CUSTOMERmembership`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`CUSTOMERmembership` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`CUSTOMERmembership` (
  `id` BIGINT UNSIGNED NOT NULL,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `level` VARCHAR(32) NULL DEFAULT '',
  `membershipCode` VARCHAR(32) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  `libLoyaltyProgramId` BIGINT UNSIGNED NOT NULL,
  `contactId` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_CUSTOMERmembership_CUSTOMERlibloyaltyprogram1`
    FOREIGN KEY (`libLoyaltyProgramId`)
    REFERENCES `pms_db`.`CUSTOMERlibLoyaltyProgram` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_CUSTOMERmembership_CUSTOMERcontact1`
    FOREIGN KEY (`contactId`)
    REFERENCES `pms_db`.`CUSTOMERcontact` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_CUSTOMERmembership_CUSTOMERlibloyaltyprogram1_idx` ON `pms_db`.`CUSTOMERmembership` (`libLoyaltyProgramId` ASC) VISIBLE;

CREATE INDEX `fk_CUSTOMERmembership_CUSTOMERcontact1_idx` ON `pms_db`.`CUSTOMERmembership` (`contactId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `pms_db`.`SERVICESlibTender`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`SERVICESlibTender` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`SERVICESlibTender` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `paymentMethod` VARCHAR(64) NULL,
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`SERVICESpayment`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`SERVICESpayment` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`SERVICESpayment` (
  `id` BIGINT UNSIGNED NOT NULL,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `paymentAmount` DECIMAL(13,4) NULL DEFAULT 0,
  `currencyCode` VARCHAR(32) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  `libTenderId` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_SERVICESpayment_SERVICESlibtender1`
    FOREIGN KEY (`libTenderId`)
    REFERENCES `pms_db`.`SERVICESlibTender` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_SERVICESpayment_SERVICESlibtender1_idx` ON `pms_db`.`SERVICESpayment` (`libTenderId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `pms_db`.`SERVICESlibServiceItems`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`SERVICESlibServiceItems` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`SERVICESlibServiceItems` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `itemName` VARCHAR(64) NULL DEFAULT '',
  `itemCode` VARCHAR(64) NULL DEFAULT '',
  `ratePlanCode` VARCHAR(64) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`SERVICESfolioOrders`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`SERVICESfolioOrders` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`SERVICESfolioOrders` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `folioOrderType` VARCHAR(32) NULL,
  `unitCount` INT NULL DEFAULT 0,
  `unitPrice` DECIMAL(13,4) NULL DEFAULT 0,
  `fixedCost` DECIMAL(13,4) NULL DEFAULT 0,
  `postingFrequency` VARCHAR(32) NULL DEFAULT '',
  `startDate` DATE NULL,
  `endDate` DATE NULL,
  `amount` DECIMAL(13,4) NULL DEFAULT 0,
  `fixedChargesQuantity` DECIMAL(13,4) NULL DEFAULT 0,
  `ratePlanCode` VARCHAR(64) NULL DEFAULT '',
  `transferId` VARCHAR(64) NULL DEFAULT '',
  `transferDateTime` INT NULL,
  `transferOnArrival` TINYINT NULL DEFAULT 0,
  `isIncluded` TINYINT NULL DEFAULT 0,
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  `contactId` BIGINT UNSIGNED NOT NULL,
  `stayId` BIGINT UNSIGNED NOT NULL,
  `paymentId` BIGINT UNSIGNED NOT NULL,
  `libServiceItemsId` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_SERVICESfolioorders_CUSTOMERcontact1`
    FOREIGN KEY (`contactId`)
    REFERENCES `pms_db`.`CUSTOMERcontact` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_SERVICESfolioorders_RESERVATIONstay1`
    FOREIGN KEY (`stayId`)
    REFERENCES `pms_db`.`RESERVATIONstay` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_SERVICESfolioorders_SERVICESpayment1`
    FOREIGN KEY (`paymentId`)
    REFERENCES `pms_db`.`SERVICESpayment` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_SERVICESfolioorders_SERVICESlibserviceitems1`
    FOREIGN KEY (`libServiceItemsId`)
    REFERENCES `pms_db`.`SERVICESlibServiceItems` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_SERVICESfolioorders_CUSTOMERcontact1_idx` ON `pms_db`.`SERVICESfolioOrders` (`contactId` ASC) VISIBLE;

CREATE INDEX `fk_SERVICESfolioorders_RESERVATIONstay1_idx` ON `pms_db`.`SERVICESfolioOrders` (`stayId` ASC) VISIBLE;

CREATE INDEX `fk_SERVICESfolioorders_SERVICESpayment1_idx` ON `pms_db`.`SERVICESfolioOrders` (`paymentId` ASC) VISIBLE;

CREATE INDEX `fk_SERVICESfolioorders_SERVICESlibserviceitems1_idx` ON `pms_db`.`SERVICESfolioOrders` (`libServiceItemsId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONgroup`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONgroup` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONgroup` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `groupName` VARCHAR(64) NULL DEFAULT '',
  `groupNumber` VARCHAR(64) NULL DEFAULT '',
  `groupStartDate` DATE NULL,
  `groupEndDate` DATE NULL,
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`CUSTOMERlibContactType`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`CUSTOMERlibContactType` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`CUSTOMERlibContactType` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `type` VARCHAR(32) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`CUSTOMERrelationship`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`CUSTOMERrelationship` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`CUSTOMERrelationship` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `metaData` JSON NULL,
  `isPrimaryGuest` TINYINT UNSIGNED NULL,
  `dataSource` VARCHAR(24) NULL,
  `contactTypeId` BIGINT UNSIGNED NOT NULL,
  `contactId` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_CUSTOMERrelationship_CUSTOMERlibcontacttype1`
    FOREIGN KEY (`contactTypeId`)
    REFERENCES `pms_db`.`CUSTOMERlibContactType` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_CUSTOMERrelationship_CUSTOMERcontact1`
    FOREIGN KEY (`contactId`)
    REFERENCES `pms_db`.`CUSTOMERcontact` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_CUSTOMERrelationship_CUSTOMERlibcontacttype1_idx` ON `pms_db`.`CUSTOMERrelationship` (`contactTypeId` ASC) VISIBLE;

CREATE INDEX `fk_CUSTOMERrelationship_CUSTOMERcontact1_idx` ON `pms_db`.`CUSTOMERrelationship` (`contactId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONlibStayStatus`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONlibStayStatus` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONlibStayStatus` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 0,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `statusName` VARCHAR(64) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONlibRoomType`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONlibRoomType` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONlibRoomType` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 0,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `typeName` VARCHAR(64) NULL DEFAULT '',
  `typeCode` VARCHAR(32) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONlibRoomClass`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONlibRoomClass` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONlibRoomClass` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 0,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `className` VARCHAR(32) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONgroupStay`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONgroupStay` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONgroupStay` (
  `id` BIGINT UNSIGNED NOT NULL,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  `stayId` BIGINT UNSIGNED NOT NULL,
  `groupId` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_RESERVATIONgroupstay_RESERVATIONstay1`
    FOREIGN KEY (`stayId`)
    REFERENCES `pms_db`.`RESERVATIONstay` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_RESERVATIONgroupstay_RESERVATIONgroup1`
    FOREIGN KEY (`groupId`)
    REFERENCES `pms_db`.`RESERVATIONgroup` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_RESERVATIONgroupstay_RESERVATIONstay1_idx` ON `pms_db`.`RESERVATIONgroupStay` (`stayId` ASC) VISIBLE;

CREATE INDEX `fk_RESERVATIONgroupstay_RESERVATIONgroup1_idx` ON `pms_db`.`RESERVATIONgroupStay` (`groupId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONstayStatusStay`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONstayStatusStay` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONstayStatusStay` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 0,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `cancelledBy` VARCHAR(64) NULL DEFAULT '',
  `cancellationDateTime` INT NULL,
  `cancellationReasonCode` VARCHAR(32) NULL DEFAULT '',
  `cancellationReasonText` VARCHAR(155) NULL DEFAULT '',
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  `stayId` BIGINT UNSIGNED NOT NULL,
  `stayStatusId` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_RESERVATIONstaystatusstay_RESERVATIONstay1`
    FOREIGN KEY (`stayId`)
    REFERENCES `pms_db`.`RESERVATIONstay` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_RESERVATIONstaystatusstay_RESERVATIONlibstaystatus1`
    FOREIGN KEY (`stayStatusId`)
    REFERENCES `pms_db`.`RESERVATIONlibStayStatus` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_RESERVATIONstaystatusstay_RESERVATIONstay1_idx` ON `pms_db`.`RESERVATIONstayStatusStay` (`stayId` ASC) VISIBLE;

CREATE INDEX `fk_RESERVATIONstaystatusstay_RESERVATIONlibstaystatus1_idx` ON `pms_db`.`RESERVATIONstayStatusStay` (`stayStatusId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `pms_db`.`RESERVATIONroomDetails`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`RESERVATIONroomDetails` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`RESERVATIONroomDetails` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `status` TINYINT NULL DEFAULT 0,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `startDate` DATE NULL,
  `endDate` DATE NULL,
  `amount` DECIMAL(13,4) NULL DEFAULT 0,
  `ratePlanCode` VARCHAR(32) NULL DEFAULT '',
  `isBlocked` TINYINT NULL DEFAULT 0,
  `isComplimentary` TINYINT NULL DEFAULT 0,
  `isHouseUse` TINYINT NULL DEFAULT 0,
  `extraBedType` VARCHAR(32) NULL DEFAULT '',
  `extraBedCount` INT NULL DEFAULT 0,
  `metaData` JSON NULL,
  `dataSource` VARCHAR(24) NULL,
  `libRoomId` BIGINT UNSIGNED NOT NULL,
  `stayId` BIGINT UNSIGNED NOT NULL,
  `libRoomTypeId` BIGINT UNSIGNED NOT NULL,
  `libRoomClassId` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_RESERVATIONroomdetails_RESERVATIONlibroom1`
    FOREIGN KEY (`libRoomId`)
    REFERENCES `pms_db`.`RESERVATIONlibRoom` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_RESERVATIONroomdetails_RESERVATIONstay1`
    FOREIGN KEY (`stayId`)
    REFERENCES `pms_db`.`RESERVATIONstay` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_RESERVATIONroomdetails_RESERVATIONlibroomtype1`
    FOREIGN KEY (`libRoomTypeId`)
    REFERENCES `pms_db`.`RESERVATIONlibRoomType` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_RESERVATIONroomdetails_RESERVATIONlibroomclass1`
    FOREIGN KEY (`libRoomClassId`)
    REFERENCES `pms_db`.`RESERVATIONlibRoomClass` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

CREATE INDEX `fk_RESERVATIONroomdetails_RESERVATIONlibroom1_idx` ON `pms_db`.`RESERVATIONroomDetails` (`libRoomId` ASC) VISIBLE;

CREATE INDEX `fk_RESERVATIONroomdetails_RESERVATIONstay1_idx` ON `pms_db`.`RESERVATIONroomDetails` (`stayId` ASC) VISIBLE;

CREATE INDEX `fk_RESERVATIONroomdetails_RESERVATIONlibroomtype1_idx` ON `pms_db`.`RESERVATIONroomDetails` (`libRoomTypeId` ASC) VISIBLE;

CREATE INDEX `fk_RESERVATIONroomdetails_RESERVATIONlibroomclass1_idx` ON `pms_db`.`RESERVATIONroomDetails` (`libRoomClassId` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `pms_db`.`PMSDATABASEmisc`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `pms_db`.`PMSDATABASEmisc` ;

CREATE TABLE IF NOT EXISTS `pms_db`.`PMSDATABASEmisc` (
  `id` BIGINT UNSIGNED NOT NULL,
  `status` TINYINT NULL DEFAULT 7,
  `rndCode` VARCHAR(24) NULL DEFAULT '',
  `createTStamp` INT UNSIGNED NULL DEFAULT 0,
  `modTStamp` INT UNSIGNED NULL DEFAULT 0,
  `schemaVersion` FLOAT NULL,
  `etlStartTStamp` INT NULL,
  `etlEndTStamp` INT NULL,
  `etlInsertsCount` INT NULL,
  `etlUpdatesCount` INT NULL,
  `etlErrorsCount` VARCHAR(45) NULL,
  `etlLogFile` VARCHAR(45) NULL,
  `etlDuration` VARCHAR(45) NULL,
  `etlSource` VARCHAR(24) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
