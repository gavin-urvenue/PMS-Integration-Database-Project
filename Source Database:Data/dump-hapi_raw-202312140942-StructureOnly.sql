-- MySQL dump 10.13  Distrib 8.2.0, for macos13 (arm64)
--
-- Host: 127.0.0.1    Database: hapi_raw
-- ------------------------------------------------------
-- Server version	8.0.26-google

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN;
SET @@SESSION.SQL_LOG_BIN= 0;

--
-- GTID state at the beginning of the backup 
--

SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'e2dcc696-d6d4-11ec-b0c8-42010a68600a:1-28089615';

--
-- Table structure for table `hapi_raw_reservations`
--

DROP TABLE IF EXISTS `hapi_raw_reservations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `hapi_raw_reservations` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `rndcode` varchar(124) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
  `createtstamp` int unsigned NOT NULL DEFAULT '0',
  `modtstamp` int unsigned NOT NULL DEFAULT '0',
  `status` tinyint NOT NULL DEFAULT '0',
  `procesststamp` int unsigned DEFAULT '0' COMMENT 'If 1, it is awaiting update.  If not 1, Hapi has synced the data via webhook.',
  `ext_id` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT 'The Hapi unique id for the reservation. The field is named "ext_id" because "id" is already in use.',
  `ext_status` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT 'REQUESTED, RESERVED, TENTATIVE, OPTIONAL, CANCELLED, NO_SHOW, WAITLISTED, IN_HOUSE, CHECKED_OUT, UNKNOWN',
  `confirmation_number` varchar(35) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `reservation_id` varchar(35) COLLATE utf8mb4_general_ci DEFAULT NULL,
  `propertyDetails` json DEFAULT NULL,
  `extracted_property_code` varchar(135) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
  `extracted_chain_code` varchar(135) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
  `extracted_guest_id` varchar(35) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `referenceIds` json DEFAULT NULL,
  `createdDateTime_repo` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT 'The point in time when Hapi firstly created the message in the MongoDB. UTC+0 timezone.',
  `createdDateTime` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT 'The date and time the reservation was created. Formatted using ISO 8601',
  `createdBy` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `lastModifiedDateTime_repo` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT 'The DateTime of last update of a given message in the MongoDB. UTC+0 timezone.',
  `lastModifiedDateTime` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT 'The date and time the reservation was last modified, if reservation has not been modified use created date. Formatted using ISO 8601',
  `lastModifiedBy` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `receivedDateTime` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT 'The point in time when Hapi firstly receives the message from an external system. UTC+0 timezone.',
  `notificationType` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `action` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `arrival` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `departure` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `bookedUnits` json DEFAULT NULL,
  `occupiedUnits` json DEFAULT NULL,
  `occupancyDetails` json DEFAULT NULL,
  `currency` json DEFAULT NULL,
  `ratePlans` json DEFAULT NULL,
  `prices` json DEFAULT NULL,
  `discounts` json DEFAULT NULL,
  `guaranteeCode` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `paymentMethod` json DEFAULT NULL,
  `isComplimentary` tinyint(1) DEFAULT NULL,
  `segmentations` json DEFAULT NULL,
  `guests` json DEFAULT NULL,
  `doNotDisplayPrice` tinyint(1) DEFAULT NULL,
  `actualDateTimeOfArrival` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `actualDateTimeOfDeparture` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `estimatedDateTimeOfArrival` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `services` json DEFAULT NULL,
  `additionalData` json DEFAULT NULL,
  `purposeOfStay` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `sharerIds` json DEFAULT NULL,
  `cancellationDetails` json DEFAULT NULL,
  `blocks` json DEFAULT NULL,
  `profiles` json DEFAULT NULL,
  `memberships` json DEFAULT NULL,
  `comments` json DEFAULT NULL,
  `optionDate` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `estimatedDateTimeOfDeparture` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `reservationTotal` json DEFAULT NULL,
  `taxes` json DEFAULT NULL,
  `promotionCode` varchar(195) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `cancellationPolicies` json DEFAULT NULL,
  `fixedCharges` json DEFAULT NULL,
  `forecastRevenue` json DEFAULT NULL,
  `blockIds` json DEFAULT NULL,
  `requestedDeposits` json DEFAULT NULL,
  `specialRequests` json DEFAULT NULL,
  `alerts` json DEFAULT NULL,
  `transfers` json DEFAULT NULL,
  `contacts` json DEFAULT NULL,
  `isHouseUse` tinyint(1) DEFAULT NULL,
  `schemaVersion` varchar(135) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
  `command_id` varchar(135) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT 'Hapi command id that can be used to query the update request status.',
  `import_code` varchar(135) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ext_id` (`ext_id`),
  KEY `modtstamp` (`modtstamp`),
  KEY `extracted_property_code` (`extracted_property_code`,`extracted_chain_code`) USING BTREE,
  KEY `lastModifiedDateTime_repo` (`lastModifiedDateTime_repo`) USING BTREE,
  KEY `hapi_reservations_cron` (`lastModifiedDateTime_repo`,`id`,`status`,`import_code`,`extracted_property_code`,`createdBy`),
  KEY `confirmation_number_status` (`confirmation_number`,`status`),
  KEY `arrivals` (`arrival`),
  KEY `lastModifiedDateTime` (`lastModifiedDateTime`),
  KEY `arrival` (`arrival`),
  KEY `departure` (`departure`),
  KEY `createtstamp` (`createtstamp`),
  KEY `extracted_guest_id` (`extracted_guest_id`),
  KEY `procesststamp` (`procesststamp`)
) ENGINE=InnoDB AUTO_INCREMENT=705683553 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`urvenue`@`%`*/ /*!50003 TRIGGER `hapi_raw_reservations_table_insert` BEFORE INSERT ON `hapi_raw_reservations` FOR EACH ROW BEGIN
            # DBLIB_VERSION=3
			DECLARE v_id BIGINT;
			CALL `dblib`.`insert_seq`('hapi_raw', 'hapi_raw_reservations', v_id);
			SET NEW.id = v_id;
			SET NEW.rndcode = `dblib`.GenIdPass(v_id, 12, '');
            IF (NEW.`status` IS NULL) OR (NEW.`status` = 0) THEN
                SET NEW.`status` = 7;
            END IF;
            SET NEW.createtstamp = UNIX_TIMESTAMP();
            SET NEW.modtstamp = UNIX_TIMESTAMP();
		END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`urvenue`@`%`*/ /*!50003 TRIGGER `hapi_raw_reservations_table_update` BEFORE UPDATE ON `hapi_raw_reservations` FOR EACH ROW BEGIN
                # DBLIB_VERSION=3
                SET NEW.modtstamp = UNIX_TIMESTAMP();
            END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
SET @@SESSION.SQL_LOG_BIN = @MYSQLDUMP_TEMP_LOG_BIN;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2023-12-14  9:42:59
