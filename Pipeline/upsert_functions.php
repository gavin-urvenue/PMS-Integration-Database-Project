<?php

function upsertCustomerContactType($data, $dbConnection) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    $tableName = 'CUSTOMERlibContactType';

    foreach ($data as $element) {
        try {
            if (isset($element[$tableName])) {
                $type = $element[$tableName]['type'];
                $dataSource = $element[$tableName]['dataSource'];

                // Start a transaction
                $dbConnection->begin_transaction();

                // Prepare the check query
                $checkQuery = "SELECT `id` FROM `$tableName` WHERE `type` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("Failed to prepare statement: " . $dbConnection->error);
                }
                $stmt->bind_param("s", $type);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                // Upsert query
                if ($exists) {
                    // Update existing record
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ? WHERE `type` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                    }
                    $updateStmt->bind_param("ss", $dataSource, $type);
                    $updateStmt->execute();
                } else {
                    // Insert new record
                    $insertQuery = "INSERT INTO `$tableName` (`type`, `dataSource`) VALUES (?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                    }
                    $insertStmt->bind_param("ss", $type, $dataSource);
                    $insertStmt->execute();
                }

                // Commit the transaction
                $dbConnection->commit();
            } else {
                throw new Exception("Invalid data structure.");
            }
        } catch (Exception $e) {
            // Rollback the transaction and log the error
            $dbConnection->rollback();
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertCustomerContactType: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            // Re-throw the exception for further handling
            throw $e;
        }
    }
}





function upsertReservationLibRoom($data, $dbConnection) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    $tableName = 'RESERVATIONlibRoom';

    foreach ($data as $record) {
        try {
            // Extract the required fields
            $roomNumber = $record['roomNumber'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []); // Convert metaData array to JSON string

            // Start a transaction
            $dbConnection->begin_transaction();

            // Prepare the check query
            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `roomNumber` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("Failed to prepare statement: " . $dbConnection->error);
            }
            $stmt->bind_param("s", $roomNumber);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            // Upsert query
            if ($exists) {
                // Update existing record
                $updateQuery = "UPDATE `$tableName` SET `metaData` = ?, `dataSource` = ? WHERE `roomNumber` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                }
                $updateStmt->bind_param("sss", $metaData, $dataSource, $roomNumber);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                // Insert new record
                $insertQuery = "INSERT INTO `$tableName` (`roomNumber`, `metaData`, `dataSource`) VALUES (?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                }
                $insertStmt->bind_param("sss", $roomNumber, $metaData, $dataSource);
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            // Commit the transaction
            $dbConnection->commit();
        } catch (Exception $e) {
            // Rollback the transaction and log the error
            $dbConnection->rollback();
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationLibRoom: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            // Re-throw the exception for further handling
            throw $e;
        }
    }
}


function upsertReservationLibSource($data, $dbConnection) {
    // Define the error log file path
    $errorLogFile = 'error_log.txt';

    $tableName = 'RESERVATIONlibSource';

    foreach ($data as $element) {
        try {
            if (isset($element['RESERVATIONlibsource'])) {
                $record = $element['RESERVATIONlibsource'];

                $sourceName = $record['sourceName'] ?? null;
                $sourceType = $record['sourceType'] ?? null;
                $dataSource = $record['dataSource'] ?? null;
                $metaData = json_encode($record['metaData'] ?? []); // Convert metaData array to JSON string

                $dbConnection->begin_transaction();

                $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `sourceName` = ? AND `sourceType` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("ss", $sourceName, $sourceType);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_row()[0] > 0;

                if ($exists) {
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `sourceName` = ? AND `sourceType` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("ssss", $dataSource, $metaData, $sourceName, $sourceType);
                    $updateStmt->execute();
                    if ($updateStmt->error) {
                        throw new Exception("Error in update operation: " . $updateStmt->error);
                    }
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`sourceName`, `sourceType`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("ssss", $sourceName, $sourceType, $dataSource, $metaData);
                    $insertStmt->execute();
                    if ($insertStmt->error) {
                        throw new Exception("Error in insert operation: " . $insertStmt->error);
                    }
                }

                $dbConnection->commit();
            } else {
                throw new Exception("Invalid data structure.");
            }
        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationLibSource: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertReservationLibProperty($data, $dbConnection) {
    $tableName = 'RESERVATIONlibProperty';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $record) {
        try {
            $propertyCode = $record['propertyCode'] ?? null;
            $chainCode = $record['chainCode'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []); // Convert metaData array to JSON string

            $dbConnection->begin_transaction();

            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `propertyCode` = ? AND `chainCode` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("Failed to prepare statement: " . $dbConnection->error);
            }

            $stmt->bind_param("ss", $propertyCode, $chainCode);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            if ($exists) {
                $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `propertyCode` = ? AND `chainCode` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                }

                $updateStmt->bind_param("ssss", $dataSource, $metaData, $propertyCode, $chainCode);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                $insertQuery = "INSERT INTO `$tableName` (`propertyCode`, `chainCode`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                }

                $insertStmt->bind_param("ssss", $propertyCode, $chainCode, $dataSource, $metaData);
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            $dbConnection->commit();

        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationLibProperty: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertCustomerLibLoyaltyProgram($data, $dbConnection) {
    $tableName = 'CUSTOMERlibLoyaltyProgram';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $element) {
        try {
            if (isset($element['CUSTOMERlibLoyaltyProgram'])) {
                $record = $element['CUSTOMERlibLoyaltyProgram'];

                $name = $record['Name'] ?? null;
                $source = $record['Source'] ?? null;
                $dataSource = $record['dataSource'] ?? null;
                $metaData = json_encode($record['metaData'] ?? []);

                $dbConnection->begin_transaction();

                $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `name` = ? AND `source` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("ss", $name, $source);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_row()[0] > 0;

                if ($exists) {
                    $updateQuery = "UPDATE `$tableName` SET `metaData` = ?, `dataSource` = ? WHERE `name` = ? AND `source` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("ssss", $metaData, $dataSource, $name, $source);
                    $updateStmt->execute();
                    if ($updateStmt->error) {
                        throw new Exception("Error in update operation: " . $updateStmt->error);
                    }
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`name`, `source`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("ssss", $name, $source, $dataSource, $metaData);
                    $insertStmt->execute();
                    if ($insertStmt->error) {
                        throw new Exception("Error in insert operation: " . $insertStmt->error);
                    }
                }

                $dbConnection->commit();

            } else {
                throw new Exception("Invalid data structure.");
            }
        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertCustomerLibLoyaltyProgram: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertServicesLibTender($data, $dbConnection) {
    $tableName = 'SERVICESlibTender';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $record) {
        try {
            $paymentMethod = $record['paymentMethod'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []);

            $dbConnection->begin_transaction();

            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `paymentMethod` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("Failed to prepare statement: " . $dbConnection->error);
            }

            $stmt->bind_param("s", $paymentMethod);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            if ($exists) {
                $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `paymentMethod` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                }

                $updateStmt->bind_param("sss", $dataSource, $metaData, $paymentMethod);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                $insertQuery = "INSERT INTO `$tableName` (`paymentMethod`, `dataSource`, `metaData`) VALUES (?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                }

                $insertStmt->bind_param("sss", $paymentMethod, $dataSource, $metaData);
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            $dbConnection->commit();

        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertServicesLibTender: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertServicesLibServiceItems($data, $dbConnection) {
    $tableName = 'SERVICESlibServiceItems';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $record) {
        try {
            $itemName = $record['itemName'] ?? null;
            $itemCode = $record['itemCode'] ?? null;
            $ratePlanCode = $record['ratePlanCode'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []);

            $dbConnection->begin_transaction();

            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `itemName` = ? AND `itemCode` = ? AND `ratePlanCode` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("Failed to prepare statement: " . $dbConnection->error);
            }

            $stmt->bind_param("sss", $itemName, $itemCode, $ratePlanCode);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            if ($exists) {
                $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `itemName` = ? AND `itemCode` = ? AND `ratePlanCode` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                }

                $updateStmt->bind_param("sssss", $dataSource, $metaData, $itemName, $itemCode, $ratePlanCode);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                $insertQuery = "INSERT INTO `$tableName` (`itemName`, `itemCode`, `ratePlanCode`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                }

                $insertStmt->bind_param("sssss", $itemName, $itemCode, $ratePlanCode, $dataSource, $metaData);
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            $dbConnection->commit();

        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertServicesLibServiceItems: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertServicesLibFolioOrdersType($data, $dbConnection) {
    $tableName = 'SERVICESlibFolioOrdersType';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $element) {
        try {
            if (isset($element[$tableName])) {
                $record = $element[$tableName];

                $orderType = $record['orderType'] ?? null;
                if ($orderType === null || $orderType === '') {
                    error_log("Skipped a record due to missing orderType", 3, $errorLogFile);
                    continue; // Skip this record if orderType is not set or is an empty string
                }

                $metaData = json_encode($record['metaData'] ?? []); // Convert metaData array to JSON string

                $dbConnection->begin_transaction();

                $checkQuery = "SELECT `id` FROM `$tableName` WHERE `orderType` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("s", $orderType);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                if ($exists) {
                    $updateQuery = "UPDATE `$tableName` SET `metaData` = ? WHERE `orderType` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("ss", $metaData, $orderType);
                    $updateStmt->execute();
                    if ($updateStmt->error) {
                        throw new Exception("Error in update operation: " . $updateStmt->error);
                    }
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`orderType`, `metaData`) VALUES (?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("ss", $orderType, $metaData);
                    $insertStmt->execute();
                    if ($insertStmt->error) {
                        throw new Exception("Error in insert operation: " . $insertStmt->error);
                    }
                }

                $dbConnection->commit();

            } else {
                throw new Exception("Invalid data structure.");
            }
        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertServicesLibFolioOrdersType: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertReservationGroup($data, $dbConnection) {
    $tableName = 'RESERVATIONgroup';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $element) {
        try {
            if (isset($element['RESERVATIONgroup'])) {
                $record = $element['RESERVATIONgroup'];

                $groupName = $record['groupName'] ?? null;
                $groupNumber = $record['groupNumber'] ?? null;
                $groupStartDate = $record['groupStartDate'] ?? null;
                $groupEndDate = $record['groupEndDate'] ?? null;
                $dataSource = $record['dataSource'] ?? null;
                $metaData = json_encode($record['metaData'] ?? []);

                $groupStartDate = !empty($groupStartDate) ? $groupStartDate : null;
                $groupEndDate = !empty($groupEndDate) ? $groupEndDate : null;

                $dbConnection->begin_transaction();

                $stmt = $dbConnection->prepare("SELECT `id` FROM `$tableName` WHERE `groupName` = ? AND `groupNumber` = ? AND (`groupStartDate` = ? OR `groupStartDate` IS NULL) AND (`groupEndDate` = ? OR `groupEndDate` IS NULL)");
                if (!$stmt) {
                    throw new Exception("Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("ssss", $groupName, $groupNumber, $groupStartDate, $groupEndDate);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                if ($exists) {
                    $updateStmt = $dbConnection->prepare("UPDATE `$tableName` SET `metaData` = ?, `dataSource` = ? WHERE `groupName` = ? AND `groupNumber` = ? AND (`groupStartDate` = ? OR `groupStartDate` IS NULL) AND (`groupEndDate` = ? OR `groupEndDate` IS NULL)");
                    if (!$updateStmt) {
                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("ssssss", $metaData, $dataSource, $groupName, $groupNumber, $groupStartDate, $groupEndDate);
                    $updateStmt->execute();
                    if ($updateStmt->error) {
                        throw new Exception("Error in update operation: " . $updateStmt->error);
                    }
                } else {
                    $insertStmt = $dbConnection->prepare("INSERT INTO `$tableName` (`groupName`, `groupNumber`, `groupStartDate`, `groupEndDate`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?, ?, ?)");
                    if (!$insertStmt) {
                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("ssssss", $groupName, $groupNumber, $groupStartDate, $groupEndDate, $dataSource, $metaData);
                    $insertStmt->execute();
                    if ($insertStmt->error) {
                        throw new Exception("Error in insert operation: " . $insertStmt->error);
                    }
                }

                $dbConnection->commit();

            } else {
                throw new Exception("Invalid data structure.");
            }
        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationGroup: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertReservationLibStayStatus($data, $dbConnection) {
    $tableName = 'RESERVATIONlibStayStatus';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $element) {
        try {
            if (isset($element['RESERVATIONlibstaystatus'])) {
                $record = $element['RESERVATIONlibstaystatus'];

                $statusName = $record['statusName'] ?? null;
                $dataSource = $record['dataSource'] ?? null;
                $metaData = json_encode($record['metaData'] ?? []);

                $dbConnection->begin_transaction();

                // Check if a record with this status name already exists
                $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `statusName` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("s", $statusName);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_row()[0] > 0;

                // Upsert query
                if ($exists) {
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `statusName` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("sss", $dataSource, $metaData, $statusName);
                    $updateStmt->execute();
                    if ($updateStmt->error) {
                        throw new Exception("Error in update operation: " . $updateStmt->error);
                    }
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`statusName`, `dataSource`, `metaData`) VALUES (?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("sss", $statusName, $dataSource, $metaData);
                    $insertStmt->execute();
                    if ($insertStmt->error) {
                        throw new Exception("Error in insert operation: " . $insertStmt->error);
                    }
                }

                $dbConnection->commit();

            } else {
                throw new Exception("Invalid data structure.");
            }
        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationLibStayStatus: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertReservationLibRoomType($data, $dbConnection) {
    $tableName = 'RESERVATIONlibRoomType';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $element) {
        try {
            if (isset($element['ReservationLibRoomType'])) {
                $record = $element['ReservationLibRoomType'];

                $typeName = $record['typeName'] ?? null;
                $typeCode = $record['typeCode'] ?? null;
                $dataSource = $record['dataSource'] ?? null;
                $metaData = json_encode($record['metaData'] ?? []);

                $dbConnection->begin_transaction();

                // Check if a record with this combination already exists
                $checkQuery = "SELECT `id` FROM `$tableName` WHERE `typeName` = ? AND `typeCode` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("ss", $typeName, $typeCode);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                // Upsert query
                if ($exists) {
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `typeName` = ? AND `typeCode` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("ssss", $dataSource, $metaData, $typeName, $typeCode);
                    $updateStmt->execute();
                    if ($updateStmt->error) {
                        throw new Exception("Error in update operation: " . $updateStmt->error);
                    }
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`typeName`, `typeCode`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("ssss", $typeName, $typeCode, $dataSource, $metaData);
                    $insertStmt->execute();
                    if ($insertStmt->error) {
                        throw new Exception("Error in insert operation: " . $insertStmt->error);
                    }
                }

                $dbConnection->commit();

            } else {
                throw new Exception("Invalid data structure.");
            }
        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationLibRoomType: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertReservationLibRoomClass($data, $dbConnection) {
    $tableName = 'RESERVATIONlibRoomClass';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $element) {
        try {
            if (isset($element['RESERVATIONlibRoomClass'])) {
                $record = $element['RESERVATIONlibRoomClass'];

                $className = $record['className'] ?? null;
                $dataSource = $record['dataSource'] ?? null;
                $metaData = json_encode($record['metaData'] ?? []); // Convert metaData array to JSON string

                $dbConnection->begin_transaction();

                // Check if a record with this class name already exists
                $checkQuery = "SELECT `id` FROM `$tableName` WHERE `className` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("s", $className);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                // Upsert query
                if ($exists) {
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `className` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("sss", $dataSource, $metaData, $className);
                    $updateStmt->execute();
                    if ($updateStmt->error) {
                        throw new Exception("Error in update operation: " . $updateStmt->error);
                    }
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`className`, `dataSource`, `metaData`) VALUES (?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("sss", $className, $dataSource, $metaData);
                    $insertStmt->execute();
                    if ($insertStmt->error) {
                        throw new Exception("Error in insert operation: " . $insertStmt->error);
                    }
                }

                $dbConnection->commit();

            } else {
                throw new Exception("Invalid data structure.");
            }
        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationLibRoomClass: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertReservationStay($data, $dbConnection) {
    $tableName = 'RESERVATIONstay';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $element) {
        try {
            $createDateTime = $element['createDateTime'] ?? null;
            $modifyDateTime = $element['modifyDateTime'] ?? null;
            $startDate = $element['startDate'] ?? null;
            $endDate = $element['endDate'] ?? null;
            $createdBy = $element['createdBy'] ?? null;
            $extPMSConfNum = $element['extPMSConfNum'] ?? null;
            $extGuestID = $element['extGuestID'] ?? null;
            $dataSource = $element['dataSource'] ?? null;
            $libSourceId = $element['libSourceId'] ?? null;
            $libPropertyId = $element['libPropertyId'] ?? null;

            $dbConnection->begin_transaction();

            // Check if a record with this combination already exists
            $checkQuery = "SELECT `id` FROM `$tableName` WHERE `createDateTime` = ? AND `modifyDateTime` = ? AND `startDate` = ? AND `endDate` = ? AND `extPMSConfNum` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("Prepare failed: " . $dbConnection->error);
            }

            $stmt->bind_param("iisss", $createDateTime, $modifyDateTime, $startDate, $endDate, $extPMSConfNum);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_assoc();

            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `extPMSConfNum` = ?, `dataSource` = ?, `libSourceId` = ?, `libPropertyId` = ?, `createdBy` = ? WHERE `createDateTime` = ? AND `modifyDateTime` = ? AND `startDate` = ? AND `endDate` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("Prepare failed: " . $dbConnection->error);
                }

                $updateStmt->bind_param("ssiisssss", $extPMSConfNum, $dataSource, $libSourceId, $libPropertyId, $createdBy, $createDateTime, $modifyDateTime, $startDate, $endDate);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`createDateTime`, `modifyDateTime`, `startDate`, `endDate`, `extPMSConfNum`, `dataSource`, `libSourceId`, `libPropertyId`, `createdBy`, `extGuestID`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("Prepare failed: " . $dbConnection->error);
                }

                $insertStmt->bind_param("iissssiiss", $createDateTime, $modifyDateTime, $startDate, $endDate, $extPMSConfNum, $dataSource, $libSourceId, $libPropertyId, $createdBy, $extGuestID);
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            $dbConnection->commit();

        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationStay: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}




function upsertCustomerRelationship($data, $dbConnection) {
    $tableName = 'CUSTOMERrelationship';
    $errorLogFile = 'error_log.txt'; // Define the error log file path


    foreach ($data as $element) {
        $isPrimaryGuest = isset($element['isPrimaryGuest']) ? (int)$element['isPrimaryGuest'] : null;
        $dataSource = $element['dataSource'] ?? null;
        $contactTypeId = $element['contactTypeId'] ?? null;
        $contactId = $element['contactId'] ?? null;

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT `id` FROM `$tableName` WHERE `isPrimaryGuest` = ? AND `dataSource` = ? AND `contactTypeId` = ? AND `contactId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("issi", $isPrimaryGuest, $dataSource, $contactTypeId, $contactId);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_assoc();

            // Upsert query
            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `isPrimaryGuest` = ?, `dataSource` = ?, `contactTypeId` = ?, `contactId` = ? WHERE `id` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("issii", $isPrimaryGuest, $dataSource, $contactTypeId, $contactId, $exists['id']);
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`isPrimaryGuest`, `dataSource`, `contactTypeId`, `contactId`) VALUES (?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("issi", $isPrimaryGuest, $dataSource, $contactTypeId, $contactId);
            }

            // Execute the query
            if ($exists) {
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            // Commit the transaction
            $dbConnection->commit();

        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertCustomerRelationship: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}

function upsertCustomerMembership($data, $dbConnection) {
    $tableName = 'CUSTOMERmembership';
    $errorLogFile = 'error_log.txt'; // Define the error log file path


    foreach ($data as $element) {
        try {
        $level = $element['level'] ?? null;
        $membershipCode = $element['membershipCode'] ?? null;
        $dataSource = $element['dataSource'] ?? null;
        $libLoyaltyProgramId = $element['libLoyaltyProgramId'] ?? null;
        $contactId = $element['contactId'] ?? null;

        // Start a transaction
        $dbConnection->begin_transaction();


            // Check if a record with this combination already exists
            $checkQuery = "SELECT `id` FROM `$tableName` WHERE `contactId` = ? AND `libLoyaltyProgramId` = ? AND `level` = ? AND `membershipCode` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("iiss", $contactId, $libLoyaltyProgramId, $level, $membershipCode);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_assoc();

            // Upsert query
            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `level` = ?, `membershipCode` = ?, `dataSource` = ? WHERE `id` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("sssi", $level, $membershipCode, $dataSource, $exists['id']);
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`level`, `membershipCode`, `dataSource`, `libLoyaltyProgramId`, `contactId`) VALUES (?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("sssii", $level, $membershipCode, $dataSource, $libLoyaltyProgramId, $contactId);
            }

            // Execute the query
            if ($exists) {
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            // Commit the transaction
            $dbConnection->commit();

        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertCustomerMembership: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}

function upsertServicesPayment($data, $dbConnection) {
    $tableName = 'SERVICESpayment';
    $errorLogFile = 'error_log.txt'; // Define the error log file path


    foreach ($data as $element) {
        try {
        $paymentAmount = $element['paymentAmount'] ?? null;
        $currencyCode = $element['currencyCode'] ?? null;
        $dataSource = $element['dataSource'] ?? null;
        $libTenderId = $element['libTenderId'] ?? null;

        // Start a transaction
        $dbConnection->begin_transaction();

            // Construct the check query with consideration for NULL values
            $checkQuery = "SELECT COUNT(*) as count FROM `$tableName` WHERE 
                (`paymentAmount` = ? OR (? IS NULL AND `paymentAmount` IS NULL)) AND 
                (`currencyCode` = ? OR (? IS NULL AND `currencyCode` IS NULL)) AND 
                `dataSource` = ? AND 
                `libTenderId` = ?";

            $checkStmt = $dbConnection->prepare($checkQuery);
            $checkStmt->bind_param("dssdsi", $paymentAmount, $paymentAmount, $currencyCode, $currencyCode, $dataSource, $libTenderId);
            $checkStmt->execute();
            $result = $checkStmt->get_result();
            $row = $result->fetch_assoc();

            // Insert only if the record does not exist
            if ($row['count'] == 0) {
                $insertQuery = "INSERT INTO `$tableName` (`paymentAmount`, `currencyCode`, `dataSource`, `libTenderId`) VALUES (?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                }
                $insertStmt->bind_param("dssi", $paymentAmount, $currencyCode, $dataSource, $libTenderId);
                $insertStmt->execute();

                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            // Commit the transaction
            $dbConnection->commit();

        } catch (Exception $e) {
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertServicesPayment: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertCustomerContact($data, $dbConnection) {
    $tableName = 'CUSTOMERcontact';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $record) {
        // Extract the required fields
        $firstName = $record['firstName'] ?? null;
        $lastName = $record['lastName'] ?? null;
        $extGuestId = $record['extGuestId'] ?? null;

        // Other fields
        $title = $record['title'] ?? '';
        $email = $record['email'] ?? '';
        $birthDate = $record['birthDate'] ?? null;
        $languageCode = $record['languageCode'] ?? '';
        $languageFormat = $record['languageFormat'] ?? '';
        $metaData = $record['metaData'] ?? null;
        $dataSource = $record['dataSource'] ?? '';

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `firstName` = ? AND `lastName` = ? AND `extGuestId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("sss", $firstName, $lastName, $extGuestId);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            // Upsert query
            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `title` = ?, `email` = ?, `birthDate` = ?, `languageCode` = ?, `languageFormat` = ?, `metaData` = ?, `dataSource` = ? WHERE `firstName` = ? AND `lastName` = ? AND `extGuestId` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("ssssssssss", $title, $email, $birthDate, $languageCode, $languageFormat, $metaData, $dataSource, $firstName, $lastName, $extGuestId);
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`firstName`, `lastName`, `title`, `email`, `birthDate`, `languageCode`, `languageFormat`, `metaData`, `dataSource`, `extGuestId`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("ssssssssss", $firstName, $lastName, $title, $email, $birthDate, $languageCode, $languageFormat, $metaData, $dataSource, $extGuestId);
            }

            // Execute the query
            if ($exists) {
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            // Commit the transaction
            $dbConnection->commit();

        } catch (Exception $e) {
            // Rollback the transaction on error
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationRoomDetails: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;  // Re-throw the exception
        }
    }
}


function upsertReservationStayStatusStay($data, $dbConnection) {
    $tableName = 'RESERVATIONstayStatusStay';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($data as $element) {
        // Skip the record if stayId is null
        if (empty($element['stayID'])) {
            $timestamp = date('Y-m-d H:i:s');
            $logMessage = "[$timestamp] Skipped record due to null stayID: " . json_encode($element) . PHP_EOL;
            error_log($logMessage, 3, $errorLogFile);
            continue;
        }

        $cancelledBy = $element['cancelledBy'] ?? null;
        $cancellationDateTime = $element['cancellationDateTime'] ?? null;
        $cancellationReasonCode = $element['cancellationReasonCode'] ?? null;
        $cancellationReasonText = $element['cancellationReasonText'] ?? null;
        $dataSource = $element['dataSource'] ?? null;
        $stayId = $element['stayID'];
        $stayStatusId = $element['stayStatusID'] ?? null;

        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT `id` FROM `$tableName` WHERE `stayId` = ? AND `stayStatusId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("Prepare failed: " . $dbConnection->error);
            }

            $stmt->bind_param("ii", $stayId, $stayStatusId);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_assoc();

            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `cancelledBy` = ?, `cancellationDateTime` = ?, `cancellationReasonCode` = ?, `cancellationReasonText` = ?, `dataSource` = ? WHERE `stayId` = ? AND `stayStatusId` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("Prepare failed: " . $dbConnection->error);
                }

                $updateStmt->bind_param("sisssii", $cancelledBy, $cancellationDateTime, $cancellationReasonCode, $cancellationReasonText, $dataSource, $stayId, $stayStatusId);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`cancelledBy`, `cancellationDateTime`, `cancellationReasonCode`, `cancellationReasonText`, `dataSource`, `stayId`, `stayStatusId`) VALUES (?, ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("Prepare failed: " . $dbConnection->error);
                }

                $insertStmt->bind_param("sisssii", $cancelledBy, $cancellationDateTime, $cancellationReasonCode, $cancellationReasonText, $dataSource, $stayId, $stayStatusId);
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            // Commit the transaction
            $dbConnection->commit();

        } catch (Exception $e) {
            // Rollback the transaction on error
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationStayStatusStay: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;  // Re-throw the exception
        }
    }
}




function upsertReservationRoomDetails($arrRESERVATIONroomDetails, $dbConnection) {
    $tableName = 'RESERVATIONroomDetails';
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    foreach ($arrRESERVATIONroomDetails as $element) {
        // Skip the record if key fields are missing
        if (empty($element['libRoomId']) || empty($element['stayId'])) {
            $timestamp = date('Y-m-d H:i:s');
            $logMessage = "[$timestamp] Skipped record due to missing key fields: " . json_encode($element) . PHP_EOL;
            error_log($logMessage, 3, $errorLogFile);
            continue;
        }

        // Extract data from the element
        $startDate = $element['startDate'];
        $endDate = $element['endDate'];
        $amount = $element['amount'];
        $ratePlanCode = $element['ratePlanCode'];
        $isBlocked = $element['isBlocked'];
        $isComplimentary = $element['isComplimentary'];
        $isHouseUse = $element['isHouseUse'];
        $dataSource = $element['dataSource'];
        $libRoomId = $element['libRoomId'];
        $stayId = $element['stayId'];
        $libRoomTypeId = $element['libRoomTypeId'];
        $libRoomClassId = $element['libRoomClassId'];

        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT `id` FROM `$tableName` WHERE `libRoomId` = ? AND `stayId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("Prepare failed: " . $dbConnection->error);
            }

            $stmt->bind_param("ii", $libRoomId, $stayId);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_assoc();

            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `startDate` = ?, `endDate` = ?, `amount` = ?, `ratePlanCode` = ?, `isBlocked` = ?, `isComplimentary` = ?, `isHouseUse` = ?, `dataSource` = ?, `libRoomTypeId` = ?, `libRoomClassId` = ? WHERE `libRoomId` = ? AND `stayId` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("Prepare failed: " . $dbConnection->error);
                }

                $updateStmt->bind_param("ssdsiiisiiii", $startDate, $endDate, $amount, $ratePlanCode, $isBlocked, $isComplimentary, $isHouseUse, $dataSource, $libRoomTypeId, $libRoomClassId, $libRoomId, $stayId);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`startDate`, `endDate`, `amount`, `ratePlanCode`, `isBlocked`, `isComplimentary`, `isHouseUse`, `dataSource`, `libRoomId`, `stayId`, `libRoomTypeId`, `libRoomClassId`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("Prepare failed: " . $dbConnection->error);
                }

                $insertStmt->bind_param("ssdsiiisiiii", $startDate, $endDate, $amount, $ratePlanCode, $isBlocked, $isComplimentary, $isHouseUse, $dataSource, $libRoomId, $stayId, $libRoomTypeId, $libRoomClassId);
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("Error in insert operation: " . $insertStmt->error);
                }
            }

            // Commit the transaction
            $dbConnection->commit();

        } catch (Exception $e) {
            // Rollback the transaction on error
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertReservationRoomDetails: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;  // Re-throw the exception
        }
    }
}

function upsertSERVICESfolioOrders($arrSERVICESfolioOrders, $dbConnection) {
    $tableName = 'SERVICESfolioOrders';
    $errorLogFile = 'error_log.txt'; // Define the error log file path



    foreach ($arrSERVICESfolioOrders as $order) {
        // Extract the fields that will be used for matching existing records
        $contactId = $order['contactId'];
        $stayId = $order['stayId'];
        $paymentId = $order['paymentId'];
        $libServiceItemsId = $order['libServiceItemsId'];
        $libFolioOrdersTypeId = $order['libFolioOrdersTypeId'];
        // ... Other fields as necessary

        $dbConnection->begin_transaction();
        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT `id` FROM `$tableName` WHERE `contactId` = ? AND `stayId` = ? AND `paymentId` = ? AND `libServiceItemsId` = ? AND `libFolioOrdersTypeId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("Prepare failed: " . $dbConnection->error);
            }

            $stmt->bind_param("iiiii", $contactId, $stayId, $paymentId, $libServiceItemsId, $libFolioOrdersTypeId);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_assoc();

            if ($exists) {
                // Update existing record
                $updateQuery = "UPDATE `$tableName` SET 
                    `folioOrderType` = ?, 
                    `unitCount` = ?, 
                    `unitPrice` = ?, 
                    `fixedCost` = ?, 
                    `postingFrequency` = ?, 
                    `startDate` = ?, 
                    `endDate` = ?, 
                    `amount` = ?, 
                    `fixedChargesQuantity` = ?, 
                    `ratePlanCode` = ?, 
                    `transferId` = ?, 
                    `transferDateTime` = ?, 
                    `transferisOnArrival` = ?, 
                    `isIncluded` = ?
                WHERE `contactId` = ? AND `stayId` = ? AND `paymentId` = ? AND `libServiceItemsId` = ? AND `libFolioOrdersTypeId` = ?";

                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("siddsssdisisiisiiiii",
                    $order['folioOrderType'],
                    $order['unitCount'],
                    $order['unitPrice'],
                    $order['fixedCost'],
                    $order['postingFrequency'],
                    $order['startDate'],
                    $order['endDate'],
                    $order['amount'],
                    $order['fixedChargesQuantity'],--
                    $order['ratePlanCode'],
                    $order['transferId'],
                    $order['transferDateTime'],
                    $order['transferisOnArrival'],
                    $order['isIncluded'],
                    $order['dataSource'],
                    // Where conditions
                    $contactId,
                    $stayId,
                    $paymentId,
                    $libServiceItemsId,
                    $libFolioOrdersTypeId
                );
                $updateStmt->execute();
            } else {
                // Insert new record
                $insertQuery = "INSERT INTO `$tableName` (
                    `folioOrderType`, 
                    `unitCount`, 
                    `unitPrice`, 
                    `fixedCost`, 
                    `postingFrequency`, 
                    `startDate`, 
                    `endDate`, 
                    `amount`, 
                    `fixedChargesQuantity`, 
                    `ratePlanCode`, 
                    `transferId`, 
                    `transferDateTime`, 
                    `transferisOnArrival`, 
                    `isIncluded`,
                    `dataSource`,
                    `contactId`, 
                    `stayId`, 
                    `paymentId`, 
                    `libServiceItemsId`, 
                    `libFolioOrdersTypeId`
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("siddsssdisisiisiiiii",
                    $order['folioOrderType'],
                    $order['unitCount'],
                    $order['unitPrice'],
                    $order['fixedCost'],
                    $order['postingFrequency'],
                    $order['startDate'],
                    $order['endDate'],
                    $order['amount'],
                    $order['fixedChargesQuantity'],
                    $order['ratePlanCode'],
                    $order['transferId'],
                    $order['transferDateTime'],
                    $order['transferOnArrival'],
                    $order['isIncluded'],
                    $order['dataSource'],
                    // Inserted values
                    $contactId,
                    $stayId,
                    $paymentId,
                    $libServiceItemsId,
                    $libFolioOrdersTypeId
                );
                $insertStmt->execute();
            }

            // Commit the transaction
            $dbConnection->commit();

        } catch (Exception $e) {
            // Rollback the transaction on error
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] Error in upsertSERVICESfolioOrders: " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;  // Re-throw the exception
        }
    }
}

?>