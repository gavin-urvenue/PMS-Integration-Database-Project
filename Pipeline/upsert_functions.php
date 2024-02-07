<?php
//These are all of the functions having to do with the upsert of data into the
//various tables of the end OLTP database

error_reporting(E_ALL);
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);


function upsertCustomerContactType($data, $dbConnection, &$errorCount) {
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    $tableName = 'CUSTOMERlibContactType';

    foreach ($data as $element) {
        try {
            if (isset($element[$tableName])) {
                $type = $element[$tableName]['type'];
                $dataSource = $element[$tableName]['dataSource'];

                // Start a transaction
                $dbConnection->begin_transaction();

                // Prepare the check query
                $checkQuery = "SELECT 1 FROM `$tableName` WHERE `type` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception(" (".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
                }
                $stmt->bind_param("s", $type);
                $stmt->execute();
                $result = $stmt->get_result();
                $existingRecord = $result->fetch_assoc();

                // Initialize action variable
                $action = '';
                $rowsAffected = 0;
                $beforeState = null;
                $afterState = null;

                if ($existingRecord) {
                    // Save before state
                    $beforeState = json_encode($existingRecord);

                    // Update existing record
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ? WHERE `type` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
                    }
                    $updateStmt->bind_param("ss", $dataSource, $type);
                    $updateStmt->execute();

                    // Set action to update and get affected rows
                    $action = 'UPDATE';
                    $rowsAffected = $updateStmt->affected_rows;

                    // Save after state
                    $afterState = json_encode(['dataSource' => $dataSource]);
                } else {
                    // Insert new record
                    $insertQuery = "INSERT INTO `$tableName` (`type`, `dataSource`) VALUES (?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception(" (".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                    }
                    $insertStmt->bind_param("ss", $type, $dataSource);
                    $insertStmt->execute();

                    // Set action to insert and get affected rows
                    $action = 'INSERT';
                    $rowsAffected = $insertStmt->affected_rows;

                    // Save after state for insert
                    $afterState = json_encode(['type' => $type, 'dataSource' => $dataSource]);
                }

                // Log the action with record identifier and rows affected
                $actionTimestamp = date('Y-m-d H:i:s');
                $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
                $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
                error_log($actionLogMessage, 3, $actionLogFile);

                // Commit the transaction
                $dbConnection->commit();
            } else {
                throw new Exception(" (".__FUNCTION__.") Invalid data structure.");
            }
        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            // Rollback the transaction and log the error
            $dbConnection->rollback();
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}]  (" . __FUNCTION__ . ") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            // Re-throw the exception for further handling
            throw $e;
        }
    }
}






function upsertReservationLibRoom($data, $dbConnection, &$errorCount) {
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    $tableName = 'RESERVATIONlibRoom';

    foreach ($data as $record) {
        try {
            // Extract the required fields
            $roomNumber = $record['roomNumber'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []); // Convert metaData array to JSON string

            $dbConnection->begin_transaction();

            // Check for existing record
            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `roomNumber` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception(" (".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
            }
            $stmt->bind_param("s", $roomNumber);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state
                $beforeState = json_encode($existingRecord);

                // Update existing record
                $updateQuery = "UPDATE `$tableName` SET `metaData` = ?, `dataSource` = ? WHERE `roomNumber` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception(" (".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                }
                $updateStmt->bind_param("sss", $metaData, $dataSource, $roomNumber);
                $updateStmt->execute();

                // Set action to update and get affected rows
                $action = 'UPDATE';
                $rowsAffected = $updateStmt->affected_rows;

                // Save after state
                $afterState = json_encode(['metaData' => $metaData, 'dataSource' => $dataSource]);
            } else {
                // Insert new record
                $insertQuery = "INSERT INTO `$tableName` (`roomNumber`, `metaData`, `dataSource`) VALUES (?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception(" (".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                }
                $insertStmt->bind_param("sss", $roomNumber, $metaData, $dataSource);
                $insertStmt->execute();

                // Set action to insert and get affected rows
                $action = 'INSERT';
                $rowsAffected = $insertStmt->affected_rows;

                // Save after state for insert
                $afterState = json_encode(['roomNumber' => $roomNumber, 'metaData' => $metaData, 'dataSource' => $dataSource]);
            }

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A';
            $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
            error_log($actionLogMessage, 3, $actionLogFile);

            $dbConnection->commit();
        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.")  " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}




function upsertReservationLibSource($data, $dbConnection, &$errorCount) {
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

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

                $checkQuery = "SELECT 1 FROM `$tableName` WHERE `sourceName` = ? AND `sourceType` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception(" (".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("ss", $sourceName, $sourceType);
                $stmt->execute();
                $result = $stmt->get_result();
                $existingRecord = $result->fetch_assoc();

                // Initialize action variable
                $action = '';
                $rowsAffected = 0;
                $beforeState = null;
                $afterState = null;

                if ($existingRecord) {
                    // Save before state
                    $beforeState = json_encode($existingRecord);

                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `sourceName` = ? AND `sourceType` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception(" (".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("ssss", $dataSource, $metaData, $sourceName, $sourceType);
                    $updateStmt->execute();

                    // Set action to update and get affected rows
                    $action = 'UPDATE';
                    $rowsAffected = $updateStmt->affected_rows;

                    // Save after state
                    $afterState = json_encode(['dataSource' => $dataSource, 'metaData' => $metaData]);
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`sourceName`, `sourceType`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception(" (".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("ssss", $sourceName, $sourceType, $dataSource, $metaData);
                    $insertStmt->execute();

                    // Set action to insert and get affected rows
                    $action = 'INSERT';
                    $rowsAffected = $insertStmt->affected_rows;

                    // Save after state for insert
                    $afterState = json_encode(['sourceName' => $sourceName, 'sourceType' => $sourceType, 'dataSource' => $dataSource, 'metaData' => $metaData]);
                }

                // Log the action with record identifier and rows affected
                $actionTimestamp = date('Y-m-d H:i:s');
                $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A';
                $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
                error_log($actionLogMessage, 3, $actionLogFile);

                $dbConnection->commit();
            } else {
                throw new Exception(" (".__FUNCTION__.") Invalid data structure.");
            }
        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.")  " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}




function upsertReservationLibProperty($data, $dbConnection, &$errorCount) {
    $tableName = 'RESERVATIONlibProperty';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $record) {
        try {
            $propertyCode = $record['propertyCode'] ?? null;
            $chainCode = $record['chainCode'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []); // Convert metaData array to JSON string

            $dbConnection->begin_transaction();

            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `propertyCode` = ? AND `chainCode` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception(" (".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
            }

            $stmt->bind_param("ss", $propertyCode, $chainCode);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state
                $beforeState = json_encode($existingRecord);

                $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `propertyCode` = ? AND `chainCode` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception(" (".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                }

                $updateStmt->bind_param("ssss", $dataSource, $metaData, $propertyCode, $chainCode);
                $updateStmt->execute();

                // Set action to update and get affected rows
                $action = 'UPDATE';
                $rowsAffected = $updateStmt->affected_rows;

                // Save after state
                $afterState = json_encode(['dataSource' => $dataSource, 'metaData' => $metaData]);
            } else {
                $insertQuery = "INSERT INTO `$tableName` (`propertyCode`, `chainCode`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception(" (".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                }

                $insertStmt->bind_param("ssss", $propertyCode, $chainCode, $dataSource, $metaData);
                $insertStmt->execute();

                // Set action to insert and get affected rows
                $action = 'INSERT';
                $rowsAffected = $insertStmt->affected_rows;

                // Save after state for insert
                $afterState = json_encode(['propertyCode' => $propertyCode, 'chainCode' => $chainCode, 'dataSource' => $dataSource, 'metaData' => $metaData]);
            }

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A';
            $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
            error_log($actionLogMessage, 3, $actionLogFile);

            $dbConnection->commit();

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.")  " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertCustomerLibLoyaltyProgram($data, $dbConnection, &$errorCount) {
    $tableName = 'CUSTOMERlibLoyaltyProgram';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $element) {
        try {
            if (isset($element['CUSTOMERlibLoyaltyProgram'])) {
                $record = $element['CUSTOMERlibLoyaltyProgram'];

                $name = $record['Name'] ?? null;
                $source = $record['Source'] ?? null;
                $dataSource = $record['dataSource'] ?? null;
                $metaData = json_encode($record['metaData'] ?? []);

                $dbConnection->begin_transaction();

                $checkQuery = "SELECT 1 FROM `$tableName` WHERE `name` = ? AND `source` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception(" (".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("ss", $name, $source);
                $stmt->execute();
                $result = $stmt->get_result();
                $existingRecord = $result->fetch_assoc();

                // Initialize action variable
                $action = '';
                $rowsAffected = 0;
                $beforeState = null;
                $afterState = null;

                if ($existingRecord) {
                    // Save before state
                    $beforeState = json_encode($existingRecord);

                    $updateQuery = "UPDATE `$tableName` SET `metaData` = ?, `dataSource` = ? WHERE `name` = ? AND `source` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception(" (".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("ssss", $metaData, $dataSource, $name, $source);
                    $updateStmt->execute();

                    // Set action to update and get affected rows
                    $action = 'UPDATE';
                    $rowsAffected = $updateStmt->affected_rows;

                    // Save after state
                    $afterState = json_encode(['metaData' => $metaData, 'dataSource' => $dataSource]);
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`name`, `source`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception(" (".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("ssss", $name, $source, $dataSource, $metaData);
                    $insertStmt->execute();

                    // Set action to insert and get affected rows
                    $action = 'INSERT';
                    $rowsAffected = $insertStmt->affected_rows;

                    // Save after state for insert
                    $afterState = json_encode(['name' => $name, 'source' => $source, 'dataSource' => $dataSource, 'metaData' => $metaData]);
                }

                // Log the action with record identifier and rows affected
                $actionTimestamp = date('Y-m-d H:i:s');
                $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
                $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
                error_log($actionLogMessage, 3, $actionLogFile);

                $dbConnection->commit();
            } else {
                throw new Exception(" (".__FUNCTION__.") Invalid data structure.");
            }
        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertServicesLibTender($data, $dbConnection, &$errorCount) {
    $tableName = 'SERVICESlibTender';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $record) {
        try {
            $paymentMethod = $record['paymentMethod'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []);

            $dbConnection->begin_transaction();

            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `paymentMethod` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("Failed to prepare statement: " . $dbConnection->error);
            }

            $stmt->bind_param("s", $paymentMethod);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state
                $beforeState = json_encode($existingRecord);

                $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `paymentMethod` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("(".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                }

                $updateStmt->bind_param("sss", $dataSource, $metaData, $paymentMethod);
                $updateStmt->execute();

                // Set action to update and get affected rows
                $action = 'UPDATE';
                $rowsAffected = $updateStmt->affected_rows;

                // Save after state
                $afterState = json_encode(['dataSource' => $dataSource, 'metaData' => $metaData]);
            } else {
                $insertQuery = "INSERT INTO `$tableName` (`paymentMethod`, `dataSource`, `metaData`) VALUES (?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("(".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                }

                $insertStmt->bind_param("sss", $paymentMethod, $dataSource, $metaData);
                $insertStmt->execute();

                // Set action to insert and get affected rows
                $action = 'INSERT';
                $rowsAffected = $insertStmt->affected_rows;

                // Save after state for insert
                $afterState = json_encode(['paymentMethod' => $paymentMethod, 'dataSource' => $dataSource, 'metaData' => $metaData]);
            }

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
            $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
            error_log($actionLogMessage, 3, $actionLogFile);

            $dbConnection->commit();

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertServicesLibServiceItems($data, $dbConnection, &$errorCount) {
    $tableName = 'SERVICESlibServiceItems';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $record) {
        try {
            $itemName = $record['itemName'] ?? null;
            $itemCode = $record['itemCode'] ?? null;
            $ratePlanCode = $record['ratePlanCode'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []);

            $dbConnection->begin_transaction();

            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `itemName` = ? AND `itemCode` = ? AND `ratePlanCode` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("(".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
            }

            $stmt->bind_param("sss", $itemName, $itemCode, $ratePlanCode);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state
                $beforeState = json_encode($existingRecord);

                $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `itemName` = ? AND `itemCode` = ? AND `ratePlanCode` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("(".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                }

                $updateStmt->bind_param("sssss", $dataSource, $metaData, $itemName, $itemCode, $ratePlanCode);
                $updateStmt->execute();

                // Set action to update and get affected rows
                $action = 'UPDATE';
                $rowsAffected = $updateStmt->affected_rows;

                // Save after state
                $afterState = json_encode(['dataSource' => $dataSource, 'metaData' => $metaData]);
            } else {
                $insertQuery = "INSERT INTO `$tableName` (`itemName`, `itemCode`, `ratePlanCode`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("(".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                }

                $insertStmt->bind_param("sssss", $itemName, $itemCode, $ratePlanCode, $dataSource, $metaData);
                $insertStmt->execute();

                // Set action to insert and get affected rows
                $action = 'INSERT';
                $rowsAffected = $insertStmt->affected_rows;

                // Save after state for insert
                $afterState = json_encode(['itemName' => $itemName, 'itemCode' => $itemCode, 'ratePlanCode' => $ratePlanCode, 'dataSource' => $dataSource, 'metaData' => $metaData]);
            }

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
            $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
            error_log($actionLogMessage, 3, $actionLogFile);

            $dbConnection->commit();

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



//function upsertServicesLibFolioOrdersType($data, $dbConnection, &$errorCount) {
//    $tableName = 'SERVICESlibFolioOrdersType';
//    $errorLogFile = 'error_log.txt'; // Define the error log file path
//
//    foreach ($data as $element) {
//        try {
//            if (isset($element[$tableName])) {
//                $record = $element[$tableName];
//
//                $orderType = $record['orderType'] ?? null;
//                if ($orderType === null || $orderType === '') {
//                    error_log("Skipped a record due to missing orderType", 3, $errorLogFile);
//                    continue; // Skip this record if orderType is not set or is an empty string
//                }
//
//                $metaData = json_encode($record['metaData'] ?? []); // Convert metaData array to JSON string
//
//                $dbConnection->begin_transaction();
//
//                $checkQuery = "SELECT `id` FROM `$tableName` WHERE `orderType` = ?";
//                $stmt = $dbConnection->prepare($checkQuery);
//                if (!$stmt) {
//                    throw new Exception("Failed to prepare statement: " . $dbConnection->error);
//                }
//
//                $stmt->bind_param("s", $orderType);
//                $stmt->execute();
//                $result = $stmt->get_result();
//                $exists = $result->fetch_assoc();
//
//                if ($exists) {
//                    $updateQuery = "UPDATE `$tableName` SET `metaData` = ? WHERE `orderType` = ?";
//                    $updateStmt = $dbConnection->prepare($updateQuery);
//                    if (!$updateStmt) {
//                        throw new Exception("Failed to prepare update statement: " . $dbConnection->error);
//                    }
//
//                    $updateStmt->bind_param("ss", $metaData, $orderType);
//                    $updateStmt->execute();
//                    if ($updateStmt->error) {
//                        throw new Exception("Error in update operation: " . $updateStmt->error);
//                    }
//                } else {
//                    $insertQuery = "INSERT INTO `$tableName` (`orderType`, `metaData`) VALUES (?, ?)";
//                    $insertStmt = $dbConnection->prepare($insertQuery);
//                    if (!$insertStmt) {
//                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
//                    }
//
//                    $insertStmt->bind_param("ss", $orderType, $metaData);
//                    $insertStmt->execute();
//                    if ($insertStmt->error) {
//                        throw new Exception("Error in insert operation: " . $insertStmt->error);
//                    }
//                }
//
//                $dbConnection->commit();
//
//            } else {
//                throw new Exception("Invalid data structure.");
//            }
//        } catch (Exception $e) {
//            // Increment error counter
//            $errorCount++;
//
//            $dbConnection->rollback();
//
//            // Log the error
//            $errorTimestamp = date('Y-m-d H:i:s');
//            $errorLogMessage = "[{$errorTimestamp}] Error in upsertServicesLibFolioOrdersType: " . $e->getMessage() . PHP_EOL;
//            error_log($errorLogMessage, 3, $errorLogFile);
//
//            throw $e;
//        }
//    }
//}


function upsertReservationGroup($data, $dbConnection, &$errorCount) {
    $tableName = 'RESERVATIONgroup';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

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

                $stmt = $dbConnection->prepare("SELECT 1 FROM `$tableName` WHERE `groupName` = ? AND `groupNumber` = ? AND (`groupStartDate` = ? OR `groupStartDate` IS NULL) AND (`groupEndDate` = ? OR `groupEndDate` IS NULL)");
                if (!$stmt) {
                    throw new Exception("(".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("ssss", $groupName, $groupNumber, $groupStartDate, $groupEndDate);
                $stmt->execute();
                $result = $stmt->get_result();
                $existingRecord = $result->fetch_assoc();

                // Initialize action variable
                $action = '';
                $rowsAffected = 0;
                $beforeState = null;
                $afterState = null;

                if ($existingRecord) {
                    // Save before state
                    $beforeState = json_encode($existingRecord);

                    $updateStmt = $dbConnection->prepare("UPDATE `$tableName` SET `metaData` = ?, `dataSource` = ? WHERE `groupName` = ? AND `groupNumber` = ? AND (`groupStartDate` = ? OR `groupStartDate` IS NULL) AND (`groupEndDate` = ? OR `groupEndDate` IS NULL)");
                    if (!$updateStmt) {
                        throw new Exception("(".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("ssssss", $metaData, $dataSource, $groupName, $groupNumber, $groupStartDate, $groupEndDate);
                    $updateStmt->execute();

                    // Set action to update and get affected rows
                    $action = 'UPDATE';
                    $rowsAffected = $updateStmt->affected_rows;

                    // Save after state
                    $afterState = json_encode(['metaData' => $metaData, 'dataSource' => $dataSource]);
                } else {
                    $insertStmt = $dbConnection->prepare("INSERT INTO `$tableName` (`groupName`, `groupNumber`, `groupStartDate`, `groupEndDate`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?, ?, ?)");
                    if (!$insertStmt) {
                        throw new Exception("(".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("ssssss", $groupName, $groupNumber, $groupStartDate, $groupEndDate, $dataSource, $metaData);
                    $insertStmt->execute();

                    // Set action to insert and get affected rows
                    $action = 'INSERT';
                    $rowsAffected = $insertStmt->affected_rows;

                    // Save after state for insert
                    $afterState = json_encode(['groupName' => $groupName, 'groupNumber' => $groupNumber, 'groupStartDate' => $groupStartDate, 'groupEndDate' => $groupEndDate, 'dataSource' => $dataSource, 'metaData' => $metaData]);
                }

                // Log the action with record identifier and rows affected
                $actionTimestamp = date('Y-m-d H:i:s');
                $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
                $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
                error_log($actionLogMessage, 3, $actionLogFile);

                $dbConnection->commit();

            } else {
                throw new Exception("(".__FUNCTION__.") Invalid data structure.");
            }
        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.")  " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}




function upsertReservationLibStayStatus($data, $dbConnection, &$errorCount) {
    $tableName = 'RESERVATIONlibStayStatus';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $element) {
        try {
            if (isset($element['RESERVATIONlibstaystatus'])) {
                $record = $element['RESERVATIONlibstaystatus'];

                $statusName = $record['statusName'] ?? null;
                $dataSource = $record['dataSource'] ?? null;
                $metaData = json_encode($record['metaData'] ?? []);

                $dbConnection->begin_transaction();

                // Check if a record with this status name already exists
                $checkQuery = "SELECT 1 FROM `$tableName` WHERE `statusName` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("(".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("s", $statusName);
                $stmt->execute();
                $result = $stmt->get_result();
                $existingRecord = $result->fetch_assoc();

                // Initialize action variable
                $action = '';
                $rowsAffected = 0;
                $beforeState = null;
                $afterState = null;

                if ($existingRecord) {
                    // Save before state
                    $beforeState = json_encode($existingRecord);

                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `statusName` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("(".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("sss", $dataSource, $metaData, $statusName);
                    $updateStmt->execute();

                    // Set action to update and get affected rows
                    $action = 'UPDATE';
                    $rowsAffected = $updateStmt->affected_rows;

                    // Save after state
                    $afterState = json_encode(['dataSource' => $dataSource, 'metaData' => $metaData]);
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`statusName`, `dataSource`, `metaData`) VALUES (?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("(".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("sss", $statusName, $dataSource, $metaData);
                    $insertStmt->execute();

                    // Set action to insert and get affected rows
                    $action = 'INSERT';
                    $rowsAffected = $insertStmt->affected_rows;

                    // Save after state for insert
                    $afterState = json_encode(['statusName' => $statusName, 'dataSource' => $dataSource, 'metaData' => $metaData]);
                }

                // Log the action with record identifier and rows affected
                $actionTimestamp = date('Y-m-d H:i:s');
                $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
                $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
                error_log($actionLogMessage, 3, $actionLogFile);

                $dbConnection->commit();

            } else {
                throw new Exception("(".__FUNCTION__.") Invalid data structure.");
            }
        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.")  " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertReservationLibRoomType($data, $dbConnection, &$errorCount) {
    $tableName = 'RESERVATIONlibRoomType';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

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
                $checkQuery = "SELECT 1 FROM `$tableName` WHERE `typeName` = ? AND `typeCode` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("(".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("ss", $typeName, $typeCode);
                $stmt->execute();
                $result = $stmt->get_result();
                $existingRecord = $result->fetch_assoc();

                // Initialize action variable
                $action = '';
                $rowsAffected = 0;
                $beforeState = null;
                $afterState = null;

                if ($existingRecord) {
                    // Save before state
                    $beforeState = json_encode($existingRecord);

                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `typeName` = ? AND `typeCode` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("(".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("ssss", $dataSource, $metaData, $typeName, $typeCode);
                    $updateStmt->execute();

                    // Set action to update and get affected rows
                    $action = 'UPDATE';
                    $rowsAffected = $updateStmt->affected_rows;

                    // Save after state
                    $afterState = json_encode(['dataSource' => $dataSource, 'metaData' => $metaData]);
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`typeName`, `typeCode`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("(".__FUNCTION__.") Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("ssss", $typeName, $typeCode, $dataSource, $metaData);
                    $insertStmt->execute();

                    // Set action to insert and get affected rows
                    $action = 'INSERT';
                    $rowsAffected = $insertStmt->affected_rows;

                    // Save after state for insert
                    $afterState = json_encode(['typeName' => $typeName, 'typeCode' => $typeCode, 'dataSource' => $dataSource, 'metaData' => $metaData]);
                }

                // Log the action with record identifier and rows affected
                $actionTimestamp = date('Y-m-d H:i:s');
                $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
                $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
                error_log($actionLogMessage, 3, $actionLogFile);

                $dbConnection->commit();

            } else {
                throw new Exception("(".__FUNCTION__.") Invalid data structure.");
            }
        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertReservationLibRoomClass($data, $dbConnection, &$errorCount) {
    $tableName = 'RESERVATIONlibRoomClass';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $element) {
        try {
            if (isset($element['RESERVATIONlibRoomClass'])) {
                $record = $element['RESERVATIONlibRoomClass'];

                $className = $record['className'] ?? null;
                $dataSource = $record['dataSource'] ?? null;
                $metaData = json_encode($record['metaData'] ?? []); // Convert metaData array to JSON string

                $dbConnection->begin_transaction();

                // Check if a record with this class name already exists
                $checkQuery = "SELECT 1 FROM `$tableName` WHERE `className` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                if (!$stmt) {
                    throw new Exception("(".__FUNCTION__.") Failed to prepare statement: " . $dbConnection->error);
                }

                $stmt->bind_param("s", $className);
                $stmt->execute();
                $result = $stmt->get_result();
                $existingRecord = $result->fetch_assoc();

                // Initialize action variable
                $action = '';
                $rowsAffected = 0;
                $beforeState = null;
                $afterState = null;

                if ($existingRecord) {
                    // Save before state
                    $beforeState = json_encode($existingRecord);

                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `className` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    if (!$updateStmt) {
                        throw new Exception("(".__FUNCTION__.") Failed to prepare update statement: " . $dbConnection->error);
                    }

                    $updateStmt->bind_param("sss", $dataSource, $metaData, $className);
                    $updateStmt->execute();

                    // Set action to update and get affected rows
                    $action = 'UPDATE';
                    $rowsAffected = $updateStmt->affected_rows;

                    // Save after state
                    $afterState = json_encode(['dataSource' => $dataSource, 'metaData' => $metaData]);
                } else {
                    $insertQuery = "INSERT INTO `$tableName` (`className`, `dataSource`, `metaData`) VALUES (?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    if (!$insertStmt) {
                        throw new Exception("Failed to prepare insert statement: " . $dbConnection->error);
                    }

                    $insertStmt->bind_param("sss", $className, $dataSource, $metaData);
                    $insertStmt->execute();

                    // Set action to insert and get affected rows
                    $action = 'INSERT';
                    $rowsAffected = $insertStmt->affected_rows;

                    // Save after state for insert
                    $afterState = json_encode(['className' => $className, 'dataSource' => $dataSource, 'metaData' => $metaData]);
                }

                // Log the action with record identifier and rows affected
                $actionTimestamp = date('Y-m-d H:i:s');
                $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
                $actionLogMessage = "[{$actionTimestamp}] (" . __FUNCTION__ . ") ACTION: $action, Record: $recordIdentifier, Rows Affected: $rowsAffected, Before: $beforeState, After: $afterState" . PHP_EOL;
                error_log($actionLogMessage, 3, $actionLogFile);

                $dbConnection->commit();

            } else {
                throw new Exception("(".__FUNCTION__.") Invalid data structure.");
            }
        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertReservationStay($data, $dbConnection, &$errorCount) {
    $tableName = 'RESERVATIONstay';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $element) {
        try {
            $createDateTime = $element['createDateTime'] ?? null;
            $modifyDateTime = $element['modifyDateTime'] ?? null;
            $startDate = $element['startDate'] ?? null;
            $endDate = $element['endDate'] ?? null;
            $createdBy = $element['createdBy'] ?? null;
            $metaData = $element['metaData'] ?? null;
            $extPMSConfNum = $element['extPMSConfNum'] ?? null;
            $extReservationId = $element['reservation_id'] ?? null;
            $extGuestId = $element['extGuestId'] ?? null;
            $dataSource = $element['dataSource'] ?? null;
            $libSourceId = $element['libSourceId'] ?? null;
            $libPropertyId = $element['libPropertyId'] ?? null;

            $dbConnection->begin_transaction();

            // Check if a record with this combination already exists
            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `extPMSConfNum` = ?  AND `libPropertyId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
            }

            $stmt->bind_param("si", $extPMSConfNum, $libPropertyId);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state
                $beforeState = json_encode($existingRecord);

                // Update
                $updateQuery = "UPDATE `$tableName` SET `createDateTime` = ?, `metaData` = ?,`extPMSConfNum` = ?, `dataSource` = ?, `libSourceId` = ?, `libPropertyId` = ?, `createdBy` = ?, `modifyDateTime` = ? WHERE `startDate` = ? AND `endDate` = ? AND `extGuestId` = ? AND `extPMSConfNum` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
                }

                $updateStmt->bind_param("isssiiisssss", $createDateTime, $metaData, $extPMSConfNum, $dataSource, $libSourceId, $libPropertyId, $createdBy, $modifyDateTime, $startDate, $endDate, $extGuestId, $extPMSConfNum);
                $updateStmt->execute();

                // Set action to update and get affected rows
                $action = 'UPDATE';
                $rowsAffected = $updateStmt->affected_rows;

                // Save after state
                $afterState = json_encode(['createDateTime' => $createDateTime, 'metaData' => $metaData, 'extPMSConfNum' => $extPMSConfNum, 'dataSource' => $dataSource, 'libSourceId' => $libSourceId, 'libPropertyId' => $libPropertyId, 'createdBy' => $createdBy, 'modifyDateTime' => $modifyDateTime]);
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`createDateTime`, `metaData`, `modifyDateTime`, `startDate`, `endDate`, `extPMSConfNum`, `dataSource`, `libSourceId`, `libPropertyId`, `createdBy`, `extGuestId`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
                }

                $insertStmt->bind_param("sssssssiiss", $createDateTime, $metaData, $modifyDateTime, $startDate, $endDate, $extPMSConfNum, $dataSource, $libSourceId, $libPropertyId, $createdBy, $extGuestId);
                $insertStmt->execute();

                // Set action to insert and get affected rows
                $action = 'INSERT';
                $rowsAffected = $insertStmt->affected_rows;

                // Save after state for insert
                $afterState = json_encode(['createDateTime' => $createDateTime, 'metaData' => $metaData, 'extPMSConfNum' => $extPMSConfNum, 'dataSource' => $dataSource, 'libSourceId' => $libSourceId, 'libPropertyId' => $libPropertyId, 'createdBy' => $createdBy, 'modifyDateTime' => $modifyDateTime]);
            }

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
            $actionLogMessage = "[{$actionTimestamp}] (".__FUNCTION__.") Action: {$action}, Record ID: {$recordIdentifier}, Rows Affected: {$rowsAffected}" . PHP_EOL;
            file_put_contents($actionLogFile, $actionLogMessage, FILE_APPEND);

            $dbConnection->commit();

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertCustomerRelationship($data, $dbConnection, &$errorCount) {
    $tableName = 'CUSTOMERrelationship';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $element) {
        $isPrimaryGuest = isset($element['isPrimaryGuest']) ? (int)$element['isPrimaryGuest'] : null;
        $dataSource = $element['dataSource'] ?? null;
        $contactTypeId = $element['contactTypeId'] ?? null;
        $contactId = $element['contactId'] ?? null;

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `isPrimaryGuest` = ? AND `dataSource` = ? AND `contactTypeId` = ? AND `contactId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("issi", $isPrimaryGuest, $dataSource, $contactTypeId, $contactId);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state
                $beforeState = json_encode($existingRecord);

                // Update
                $updateQuery = "UPDATE `$tableName` SET `isPrimaryGuest` = ?, `dataSource` = ?, `contactTypeId` = ?, `contactId` = ? WHERE `id` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("issii", $isPrimaryGuest, $dataSource, $contactTypeId, $contactId, $existingRecord['id']);

                // Set action to update
                $action = 'UPDATE';
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`isPrimaryGuest`, `dataSource`, `contactTypeId`, `contactId`) VALUES (?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("issi", $isPrimaryGuest, $dataSource, $contactTypeId, $contactId);

                // Set action to insert
                $action = 'INSERT';
            }

            // Execute the query
            if ($existingRecord) {
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("(".__FUNCTION__.") Error in update operation: " . $updateStmt->error);
                }

                // Save after state
                $afterState = json_encode(['isPrimaryGuest' => $isPrimaryGuest, 'dataSource' => $dataSource, 'contactTypeId' => $contactTypeId, 'contactId' => $contactId]);
            } else {
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("(".__FUNCTION__.") Error in insert operation: " . $insertStmt->error);
                }

                // Save after state for insert
                $afterState = json_encode(['isPrimaryGuest' => $isPrimaryGuest, 'dataSource' => $dataSource, 'contactTypeId' => $contactTypeId, 'contactId' => $contactId]);
            }

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
            $actionLogMessage = "[{$actionTimestamp}] (".__FUNCTION__.") Action: {$action}, Record ID: {$recordIdentifier}, Rows Affected: {$rowsAffected}" . PHP_EOL;
            file_put_contents($actionLogFile, $actionLogMessage, FILE_APPEND);

            // Commit the transaction
            $dbConnection->commit();

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}




function upsertCustomerMembership($data, $dbConnection, &$errorCount) {
    $tableName = 'CUSTOMERmembership';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $element) {
        $level = $element['level'] ?? null;
        $membershipCode = $element['membershipCode'] ?? null;
        $dataSource = $element['dataSource'] ?? null;
        $libLoyaltyProgramId = $element['libLoyaltyProgramId'] ?? null;
        $contactId = $element['contactId'] ?? null;

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `contactId` = ? AND `libLoyaltyProgramId` = ? AND `level` = ? AND `membershipCode` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("iiss", $contactId, $libLoyaltyProgramId, $level, $membershipCode);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state
                $beforeState = json_encode($existingRecord);

                // Update
                $updateQuery = "UPDATE `$tableName` SET `level` = ?, `membershipCode` = ?, `dataSource` = ? WHERE `id` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("sssi", $level, $membershipCode, $dataSource, $existingRecord['id']);

                // Set action to update
                $action = 'UPDATE';
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`level`, `membershipCode`, `dataSource`, `libLoyaltyProgramId`, `contactId`) VALUES (?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("sssii", $level, $membershipCode, $dataSource, $libLoyaltyProgramId, $contactId);

                // Set action to insert
                $action = 'INSERT';
            }

            // Execute the query
            if ($existingRecord) {
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("(".__FUNCTION__.") Error in update operation: " . $updateStmt->error);
                }

                // Save after state
                $afterState = json_encode(['level' => $level, 'membershipCode' => $membershipCode, 'dataSource' => $dataSource, 'libLoyaltyProgramId' => $libLoyaltyProgramId, 'contactId' => $contactId]);
            } else {
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("(".__FUNCTION__.") Error in insert operation: " . $insertStmt->error);
                }

                // Save after state for insert
                $afterState = json_encode(['level' => $level, 'membershipCode' => $membershipCode, 'dataSource' => $dataSource, 'libLoyaltyProgramId' => $libLoyaltyProgramId, 'contactId' => $contactId]);
            }

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
            $actionLogMessage = "[{$actionTimestamp}] (".__FUNCTION__.") Action: {$action}, Record ID: {$recordIdentifier}, Rows Affected: {$rowsAffected}, Before State: {$beforeState}, After State: {$afterState}" . PHP_EOL;
            file_put_contents($actionLogFile, $actionLogMessage, FILE_APPEND);

            // Commit the transaction
            $dbConnection->commit();

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.")  " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}


function upsertSERVICESPayment($data, $dbConnection, &$errorCount) {
    $tableName = 'SERVICESpayment';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $element) {
        $paymentAmount = $element['paymentAmount'] ?? null;
        $currencyCode = $element['currencyCode'] ?? null;
        $dataSource = $element['dataSource'] ?? null;
        $libTenderId = $element['libTenderId'] ?? null;

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
            // Construct the check query with consideration for NULL values
            $checkQuery = "SELECT 1 FROM `$tableName` WHERE 
                `paymentAmount` = ?  AND 
                `currencyCode` = ?  AND 
                `dataSource` = ? AND 
                `libTenderId` = ?";

            $checkStmt = $dbConnection->prepare($checkQuery);
            $checkStmt->bind_param("dssi", $paymentAmount, $currencyCode, $dataSource, $libTenderId);
            $checkStmt->execute();
            $result = $checkStmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            // Insert only if the record does not exist
            if (!$existingRecord) {
                // Save before state for insert
                $beforeState = json_encode(['paymentAmount' => $paymentAmount, 'currencyCode' => $currencyCode, 'dataSource' => $dataSource, 'libTenderId' => $libTenderId]);

                $insertQuery = "INSERT INTO `$tableName` (`paymentAmount`, `currencyCode`, `dataSource`, `libTenderId`) VALUES (?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("dssi", $paymentAmount, $currencyCode, $dataSource, $libTenderId);
                $insertStmt->execute();

                if ($insertStmt->error) {
                    throw new Exception("(".__FUNCTION__.") " . $insertStmt->error);
                }

                // Set action to insert
                $action = 'INSERT';
            }

            // Commit the transaction
            $dbConnection->commit();

            // Save after state for insert
            $afterState = json_encode(['paymentAmount' => $paymentAmount, 'currencyCode' => $currencyCode, 'dataSource' => $dataSource, 'libTenderId' => $libTenderId]);

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = $existingRecord ? $existingRecord['id'] : 'N/A'; // Use 'N/A' for new records
            $actionLogMessage = "[{$actionTimestamp}] (".__FUNCTION__.") Action: {$action}, Record ID: {$recordIdentifier}, Rows Affected: {$rowsAffected}, Before State: {$beforeState}, After State: {$afterState}" . PHP_EOL;
            file_put_contents($actionLogFile, $actionLogMessage, FILE_APPEND);

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.")  ". $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;
        }
    }
}



function upsertCustomerContact($data, $dbConnection, &$errorCount) {
    $tableName = 'CUSTOMERcontact';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

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
            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `firstName` = ? AND `lastName` = ? AND `extGuestId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("sss", $firstName, $lastName, $extGuestId);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            // Upsert query
            if ($existingRecord) {
                // Save before state for update
                $beforeState = json_encode($existingRecord);

                // Update
                $updateQuery = "UPDATE `$tableName` SET `title` = ?, `email` = ?, `birthDate` = ?, `languageCode` = ?, `languageFormat` = ?, `metaData` = ?, `dataSource` = ? WHERE `firstName` = ? AND `lastName` = ? AND `extGuestId` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("ssssssssss", $title, $email, $birthDate, $languageCode, $languageFormat, $metaData, $dataSource, $firstName, $lastName, $extGuestId);
                $updateStmt->execute();

                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }

                // Set action to update
                $action = 'UPDATE';
                $rowsAffected = $updateStmt->affected_rows;
            } else {
                // Save before state for insert
                $beforeState = json_encode([
                    'firstName' => $firstName,
                    'lastName' => $lastName,
                    'extGuestId' => $extGuestId,
                    'title' => '',
                    'email' => '',
                    'birthDate' => null,
                    'languageCode' => '',
                    'languageFormat' => '',
                    'metaData' => null,
                    'dataSource' => '',
                ]);

                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`firstName`, `lastName`, `title`, `email`, `birthDate`, `languageCode`, `languageFormat`, `metaData`, `dataSource`, `extGuestId`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("ssssssssss", $firstName, $lastName, $title, $email, $birthDate, $languageCode, $languageFormat, $metaData, $dataSource, $extGuestId);
                $insertStmt->execute();

                if ($insertStmt->error) {
                    throw new Exception("(".__FUNCTION__.") Error in insert operation: " . $insertStmt->error);
                }

                // Set action to insert
                $action = 'INSERT';
                $rowsAffected = $insertStmt->affected_rows;
            }

            // Commit the transaction
            $dbConnection->commit();

            // Save after state for insert or update
            $afterState = json_encode([
                'firstName' => $firstName,
                'lastName' => $lastName,
                'extGuestId' => $extGuestId,
                'title' => $title,
                'email' => $email,
                'birthDate' => $birthDate,
                'languageCode' => $languageCode,
                'languageFormat' => $languageFormat,
                'metaData' => $metaData,
                'dataSource' => $dataSource,
            ]);

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = "{$firstName}, {$lastName}, {$extGuestId}";
            $actionLogMessage = "[{$actionTimestamp}] (".__FUNCTION__.") Action: {$action}, Record ID: {$recordIdentifier}, Rows Affected: {$rowsAffected}, Before State: {$beforeState}, After State: {$afterState}" . PHP_EOL;
            file_put_contents($actionLogFile, $actionLogMessage, FILE_APPEND);

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            // Rollback the transaction on error
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.")  " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;  // Re-throw the exception
        }
    }
}



function upsertReservationStayStatusStay($data, $dbConnection, &$errorCount) {
    $tableName = 'RESERVATIONstayStatusStay';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($data as $element) {
        // Skip the record if stayId is null
        if (empty($element['stayId'])) {
            $timestamp = date('Y-m-d H:i:s');
            $logMessage = "[$timestamp] (".__FUNCTION__.") Skipped record due to null stayId: " . json_encode($element) . PHP_EOL;
            error_log($logMessage, 3, $errorLogFile);
            continue;
        }

        $cancelledBy = $element['cancelledBy'] ?? null;
        $cancellationDateTime = $element['cancellationDateTime'] ?? null;
        $cancellationReasonCode = $element['cancellationReasonCode'] ?? null;
        $cancellationReasonText = $element['cancellationReasonText'] ?? null;
        $dataSource = $element['dataSource'] ?? null;
        $stayId = $element['stayId'];
        $libStayStatusId = $element['libStayStatusId'] ?? null;

        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `stayId` = ? AND `libStayStatusId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
            }

            $stmt->bind_param("ii", $stayId, $libStayStatusId);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state for update
                $beforeState = json_encode($existingRecord);

                // Update
                $updateQuery = "UPDATE `$tableName` SET `cancelledBy` = ?, `cancellationDateTime` = ?, `cancellationReasonCode` = ?, `cancellationReasonText` = ?, `dataSource` = ? WHERE `stayId` = ? AND `libStayStatusId` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
                }

                $updateStmt->bind_param("sisssii", $cancelledBy, $cancellationDateTime, $cancellationReasonCode, $cancellationReasonText, $dataSource, $stayId, $libStayStatusId);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("(".__FUNCTION__.") Error in update operation: " . $updateStmt->error);
                }

                // Set action to update
                $action = 'UPDATE';
                $rowsAffected = $updateStmt->affected_rows;
            } else {
                // Save before state for insert
                $beforeState = json_encode([
                    'stayId' => $stayId,
                    'libStayStatusId' => $libStayStatusId,
                    'cancelledBy' => null,
                    'cancellationDateTime' => null,
                    'cancellationReasonCode' => null,
                    'cancellationReasonText' => null,
                    'dataSource' => null,
                ]);

                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`stayId`, `libStayStatusId`, `cancelledBy`, `cancellationDateTime`, `cancellationReasonCode`, `cancellationReasonText`, `dataSource`) VALUES (?, ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
                }

                $insertStmt->bind_param("iisssss", $stayId, $libStayStatusId, $cancelledBy, $cancellationDateTime, $cancellationReasonCode, $cancellationReasonText, $dataSource);
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("(".__FUNCTION__.") Error in insert operation: " . $insertStmt->error);
                }

                // Set action to insert
                $action = 'INSERT';
                $rowsAffected = $insertStmt->affected_rows;
            }

            // Commit the transaction
            $dbConnection->commit();

            // Save after state for insert or update
            $afterState = json_encode([
                'stayId' => $stayId,
                'libStayStatusId' => $libStayStatusId,
                'cancelledBy' => $cancelledBy,
                'cancellationDateTime' => $cancellationDateTime,
                'cancellationReasonCode' => $cancellationReasonCode,
                'cancellationReasonText' => $cancellationReasonText,
                'dataSource' => $dataSource,
            ]);

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = "stayId: {$stayId}, libStayStatusId: {$libStayStatusId}";
            $actionLogMessage = "[{$actionTimestamp}] (".__FUNCTION__.") Action: {$action}, Record ID: {$recordIdentifier}, Rows Affected: {$rowsAffected}, Before State: {$beforeState}, After State: {$afterState}" . PHP_EOL;
            file_put_contents($actionLogFile, $actionLogMessage, FILE_APPEND);

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            // Rollback the transaction on error
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;  // Re-throw the exception
        }
    }
}





function upsertReservationRoomDetails($arrRESERVATIONroomDetails, $dbConnection, &$errorCount) {
    $tableName = 'RESERVATIONroomDetails';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($arrRESERVATIONroomDetails as $element) {
        // Skip the record if key fields are missing
        if (empty($element['libRoomId']) || empty($element['stayId'])) {
            $timestamp = date('Y-m-d H:i:s');
            $logMessage = "[$timestamp] (".__FUNCTION__.")  Skipped record due to missing key fields: " . json_encode($element) . PHP_EOL;
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
        $metaData = $element['metaData'];

        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT 1 FROM `$tableName` WHERE `startDate` = ? AND `endDate` = ? AND `amount` = ? AND `ratePlanCode` = ? AND `isBlocked` = ?
            AND `isComplimentary` = ? AND `isHouseUse` = ? AND `metaData` = ? AND `dataSource` = ? AND `libRoomId` = ? AND `stayId` = ? AND `libRoomTypeId` = ?
            AND `libRoomClassId` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
            }

            $stmt->bind_param("ssdsiiissiiii", $startDate, $endDate, $amount, $ratePlanCode, $isBlocked, $isComplimentary, $isHouseUse, $metaData, $dataSource, $libRoomId, $stayId, $libRoomTypeId,
            $libRoomClassId);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state for update
                $beforeState = json_encode($existingRecord);

                // Update
                $updateQuery = "UPDATE `$tableName` SET `startDate` = ? AND `endDate` = ? AND `amount` = ? AND `ratePlanCode` = ? AND `isBlocked` = ?
            AND `isComplimentary` = ? AND `isHouseUse` = ? AND `metaData` = ? AND `dataSource` = ? AND `libRoomId` = ? AND `stayId` = ? AND `libRoomTypeId` = ?
            AND `libRoomClassId` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
                }

                $updateStmt->bind_param("ssdsiiissiiii", $startDate, $endDate, $amount, $ratePlanCode, $isBlocked, $isComplimentary, $isHouseUse, $metaData, $dataSource, $libRoomId, $stayId, $libRoomTypeId,
                    $libRoomClassId);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("(".__FUNCTION__.") Error in update operation: " . $updateStmt->error);
                }

                // Set action to update
                $action = 'UPDATE';
                $rowsAffected = $updateStmt->affected_rows;
            } else {
                // Save before state for insert
                $beforeState = json_encode([
                    'libRoomId' => $libRoomId,
                    'stayId' => $stayId,
                    'startDate' => null,
                    'endDate' => null,
                    'amount' => null,
                    'ratePlanCode' => null,
                    'isBlocked' => null,
                    'isComplimentary' => null,
                    'isHouseUse' => null,
                    'dataSource' => null,
                    'libRoomTypeId' => null,
                    'libRoomClassId' => null,
                ]);

                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`startDate`, `endDate`, `amount`, `ratePlanCode`, `isBlocked`, `isComplimentary`, `isHouseUse`,
                     `metaData`, `dataSource`, `libRoomId`, `stayId`, `libRoomTypeId`, `libRoomClassId`)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
                }

                $insertStmt->bind_param("ssdsiiissiiii", $startDate, $endDate, $amount, $ratePlanCode, $isBlocked, $isComplimentary, $isHouseUse, $metaData, $dataSource, $libRoomId, $stayId, $libRoomTypeId,
                    $libRoomClassId);
                $insertStmt->execute();
                if ($insertStmt->error) {
                    throw new Exception("(".__FUNCTION__.") Error in insert operation: " . $insertStmt->error);
                }

                // Set action to insert
                $action = 'INSERT';
                $rowsAffected = $insertStmt->affected_rows;
            }

            // Commit the transaction
            $dbConnection->commit();

            // Save after state for insert or update
            $afterState = json_encode([
                'libRoomId' => $libRoomId,
                'stayId' => $stayId,
                'startDate' => $startDate,
                'endDate' => $endDate,
                'amount' => $amount,
                'ratePlanCode' => $ratePlanCode,
                'isBlocked' => $isBlocked,
                'isComplimentary' => $isComplimentary,
                'isHouseUse' => $isHouseUse,
                'dataSource' => $dataSource,
                'libRoomTypeId' => $libRoomTypeId,
                'libRoomClassId' => $libRoomClassId,
            ]);

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = "libRoomId: {$libRoomId}, stayId: {$stayId}";
            $actionLogMessage = "[{$actionTimestamp}] (".__FUNCTION__.") Action: {$action}, Record ID: {$recordIdentifier}, Rows Affected: {$rowsAffected}, Before State: {$beforeState}, After State: {$afterState}" . PHP_EOL;
            file_put_contents($actionLogFile, $actionLogMessage, FILE_APPEND);

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            // Rollback the transaction on error
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") " . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;  // Re-throw the exception
        }
    }
}


function upsertSERVICESfolioOrders($arrSERVICESfolioOrders, $dbConnection, &$errorCount) {
    $tableName = 'SERVICESfolioOrders';
    // Define the error log file path and action log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $actionLogFile = dirname(__FILE__) . '/action_log.txt';

    foreach ($arrSERVICESfolioOrders as $order) {
        // Extract the fields that will be used for matching existing records
        $folioOrderType = $order['folioOrderType'];
        $unitCount = $order['unitCount'];
        $unitPrice = $order['unitPrice'];
        $fixedCost = $order['fixedCost'];
        $amountBeforeTax = $order['amountBeforeTax'];
        $amountAfterTax = $order['amountAfterTax'];
        $postingFrequency = $order['postingFrequency'];
        $startDate = $order['startDate'];
        $endDate = $order['endDate'];
        $fixedChargesQuantity = $order['fixedChargesQuantity'];
        $transferId = $order['transferId'];
        $transferDateTime = $order['transferDateTime'];
        $transferOnArrival = $order['transferOnArrival'];
        $isIncluded = $order['isIncluded'];
        $contactId = $order['contactId'];
        $stayId = $order['stayId'];
        $paymentId = $order['paymentId'];
        $libServiceItemsId = $order['libServiceItemsId'];
        $metaData = $order['metaData'];

        $dbConnection->begin_transaction();
        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT 1 FROM `$tableName` WHERE 
            `folioOrderType` = ? AND
            `unitCount` = ? AND
            `unitPrice` = ? AND
            `fixedCost` = ? AND
            `amountBeforeTax` = ? AND
            `amountAfterTax` = ? AND
            `postingFrequency` = ? AND
            `startDate` = ? AND
            `endDate` = ? AND
            `fixedChargesQuantity` = ? AND
            `transferId` = ? AND
            `transferDateTime` = ? AND
            `transferOnArrival` = ? AND
            `isIncluded` = ? AND
            `contactId` = ? AND
            `stayId` = ? AND
            `paymentId` = ? AND
            `libServiceItemsId` = ? AND
            `metaData` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            if (!$stmt) {
                throw new Exception("(".__FUNCTION__.") Prepare failed: " . $dbConnection->error);
            }

            $stmt->bind_param("siddddsssiisiiiiiis", $folioOrderType, $unitCount, $unitPrice, $fixedCost,
                $amountBeforeTax, $amountAfterTax, $postingFrequency,
                $startDate, $endDate, $fixedChargesQuantity,
                $transferId, $transferDateTime, $transferOnArrival,
                $isIncluded, $contactId, $stayId, $paymentId,
                $libServiceItemsId, $metaData);
            $stmt->execute();
            $result = $stmt->get_result();
            $existingRecord = $result->fetch_assoc();

            // Initialize action variable
            $action = '';
            $rowsAffected = 0;
            $beforeState = null;
            $afterState = null;

            if ($existingRecord) {
                // Save before state for update
                $beforeState = json_encode($existingRecord);

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
                    `transferOnArrival` = ?,
                    `isIncluded` = ?,
                    `dataSource` = ?,
                    `metaData` = ?
                WHERE `contactId` = ? AND `stayId` = ? AND `paymentId` = ? AND `libServiceItemsId` = ?";

                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("siddsssdisssiissiiii",
                    $order['folioOrderType'],
                    $order['unitCount'],
                    $order['unitPrice'],
                    $order['fixedCost'],
                    $order['amountBeforeTax'],
                    $order['amountAfterTax'],
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
                    $order['metaData'],
                    // Where conditions
                    $contactId,
                    $stayId,
                    $paymentId,
                    $libServiceItemsId
                );
                $updateStmt->execute();

                // Set action to update
                $action = 'UPDATE';
                $rowsAffected = $updateStmt->affected_rows;
            } else {
                // Save before state for insert
                $beforeState = json_encode([
                    'contactId' => $contactId,
                    'stayId' => $stayId,
                    'paymentId' => $paymentId,
                    'libServiceItemsId' => $libServiceItemsId,
                    'folioOrderType' => null,
                    'unitCount' => null,
                    'unitPrice' => null,
                    'fixedCost' => null,
                    'postingFrequency' => null,
                    'startDate' => null,
                    'endDate' => null,
                    'amount' => null,
                    'fixedChargesQuantity' => null,
                    'ratePlanCode' => null,
                    'transferId' => null,
                    'transferDateTime' => null,
                    'transferOnArrival' => null,
                    'isIncluded' => null,
                    'dataSource' => null,
                    'metaData' => null,
                ]);

                // Insert new record
                $insertQuery = "INSERT INTO `$tableName` (
                    `contactId`,
                    `stayId`,
                    `paymentId`,
                    `libServiceItemsId`,
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
                    `transferOnArrival`, 
                    `isIncluded`,
                    `dataSource`,
                    `metaData`
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("iiiidddsssddssssiss", $contactId, $stayId, $paymentId, $libServiceItemsId,  $order['unitCount'], $order['unitPrice'], $order['fixedCost'], $order['postingFrequency'], $order['startDate'], $order['endDate'], $order['amount'], $order['fixedChargesQuantity'], $order['ratePlanCode'], $order['transferId'], $order['transferDateTime'], $order['transferOnArrival'], $order['isIncluded'], $order['dataSource'], $order['metaData']);
                $insertStmt->execute();

                // Set action to insert
                $action = 'INSERT';
                $rowsAffected = $insertStmt->affected_rows;
            }

            // Commit the transaction
            $dbConnection->commit();

            // Save after state for insert or update
            $afterState = json_encode([
                'contactId' => $contactId,
                'stayId' => $stayId,
                'paymentId' => $paymentId,
                'libServiceItemsId' => $libServiceItemsId,
                'folioOrderType' => $order['folioOrderType'],
                'unitCount' => $order['unitCount'],
                'unitPrice' => $order['unitPrice'],
                'fixedCost' => $order['fixedCost'],
                'postingFrequency' => $order['postingFrequency'],
                'startDate' => $order['startDate'],
                'endDate' => $order['endDate'],
                'amount' => $order['amount'],
                'fixedChargesQuantity' => $order['fixedChargesQuantity'],
                'ratePlanCode' => $order['ratePlanCode'],
                'transferId' => $order['transferId'],
                'transferDateTime' => $order['transferDateTime'],
                'transferOnArrival' => $order['transferOnArrival'],
                'isIncluded' => $order['isIncluded'],
                'dataSource' => $order['dataSource'],
                'metaData' => $order['metaData'],
            ]);

            // Log the action with record identifier and rows affected
            $actionTimestamp = date('Y-m-d H:i:s');
            $recordIdentifier = "contactId: {$contactId}, stayId: {$stayId}, paymentId: {$paymentId}, libServiceItemsId: {$libServiceItemsId}";
            $actionLogMessage = "[{$actionTimestamp}] (".__FUNCTION__.") Action: {$action}, Record ID: {$recordIdentifier}, Rows Affected: {$rowsAffected}, Before State: {$beforeState}, After State: {$afterState}" . PHP_EOL;
            file_put_contents($actionLogFile, $actionLogMessage, FILE_APPEND);

        } catch (Exception $e) {
            // Increment error counter
            $errorCount++;

            // Rollback the transaction on error
            $dbConnection->rollback();

            // Log the error
            $errorTimestamp = date('Y-m-d H:i:s');
            $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error: stayId:". $stayId . $e->getMessage() . PHP_EOL;
            error_log($errorLogMessage, 3, $errorLogFile);

            throw $e;  // Re-throw the exception
        }
    }
}


?>