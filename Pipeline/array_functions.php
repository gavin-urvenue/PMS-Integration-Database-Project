<?php
//These are all of the functions having to do with preparing the associative arrays for
// upsert into the final OLTP database. There are functions to create associative arrays mimicking the table structure
// of the final database, but also a function to mimick a MySQL table into an associative array, as primary key values
// are generated at the MySQL Server level.

error_reporting(E_ALL);
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

//////Function to remove duplicate records from an array based on 2 fields
//function removeDuplicateRows2D($data, $key1, $key2)
//{
//    $uniqueKeys = [];
//
//    return array_filter($data, function ($item) use ($key1, $key2, &$uniqueKeys) {
//        $value1 = $item[$key1];
//        $value2 = $item[$key2];
//
//        $hash = md5($value1 . $value2);
//
//        if (!isset($uniqueKeys[$hash])) {
//            $uniqueKeys[$hash] = true;
//            return true;
//        }
//
//        return false;
//    });
//}
//
//function removeDuplicateRows3D($data, $key1, $key2, $key3)
//{
//    $uniqueKeys = [];
//
//    return array_filter($data, function ($item) use ($key1, $key2, $key3, &$uniqueKeys) {
//        $value1 = $item[$key1];
//        $value2 = $item[$key2];
//        $value3 = $item[$key3];
//
//        $hash = md5($value1 . $value2 . $value3);
//
//        if (!isset($uniqueKeys[$hash])) {
//            $uniqueKeys[$hash] = true;
//            return true;
//        }
//
//        return false;
//    });
//}
// Function to fetch data from a MySQL table and store it in a PHP associative  array
function getLatestEtlTimestamp($destinationDBConnection)
{
    $etlStartTStamp = 0;

    try {
        $query = "SELECT etlStartTStamp FROM PMSDATABASEmisc ORDER BY id DESC LIMIT 1";
        $result = $destinationDBConnection->query($query);
        if ($result && $row = $result->fetch_assoc()) {
            $etlStartTStamp = $row['etlStartTStamp'];
        }
    } catch (Exception $e) {

        error_log("Error fetching ETL timestamp: " . $e->getMessage(), 3, 'error_log.txt');
    }

    return $etlStartTStamp;
}

function fetchDataFromMySQLTable($tableName, $originDBConnection, $destinationDBConnection, &$insertCount, &$updateCount, &$errorCount) {
    $errorLogFile = 'error_log.txt'; // Define the error log file path
    // Function to check connections to origin and destination databases and update the PMSDATABASEmisc
    // table with info on the ETL and database schema. Also pulls data from origin database/table to an
    // associative array
    try {
        // Check connection
        if ($originDBConnection->connect_error) {
            throw new Exception('Connection failed: ' . $originDBConnection->connect_error);
        }

        // Start transaction
        $originDBConnection->begin_transaction();

        // Fetch the last etlStartTStamp from PMSDATABASEmisc
        $lastEtlStartTStamp = getLatestEtlTimestamp($destinationDBConnection, $errorCount);

        // Prepare and execute query
        $query = "SELECT * FROM $tableName WHERE createTStamp > $lastEtlStartTStamp OR modTStamp > $lastEtlStartTStamp";
        $result = $originDBConnection->query($query);

        // Check for query success
        if (!$result) {
            throw new Exception('Query failed: ' . $originDBConnection->error);
        }

        // Fetch data and store it in an array
        $data = [];
        while ($row = $result->fetch_assoc()) {
            if (is_null($row['confirmation_number']))
            {
                continue;
            }
            $data[] = $row;

            // Calculate insert and update counts
            if ($row['createtstamp'] > $lastEtlStartTStamp) {
                $insertCount++;
            }
            if ($row['modtstamp'] > $lastEtlStartTStamp AND $row['createtstamp'] <= $lastEtlStartTStamp) {
                $updateCount++;
            }
        }

        // Commit the transaction
        $originDBConnection->commit();

        // Log the number of records fetched
        $recordCount = count($data);
        $errorTimestamp = date('Y-m-d H:i:s'); // Format the date and time as you prefer
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully fetched $recordCount records from $tableName" . PHP_EOL;
        error_log($successMessage, 3, 'error_log.txt');

        return $data;

    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Rollback the transaction on error
        $originDBConnection->rollback();

        // Log the error
        $errorTimestamp = date('Y-m-d H:i:s'); // Format the date and time as you prefer
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") fetchDataFromMySQLTable failed: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if you want to handle it further up the call stack
        throw $e;
    }
}


function insertEtlTrackingInfo($destinationDBConnection, $insertCount, $updateCount, $etlSource, $schemaVersion, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        // Check connection
        if ($destinationDBConnection->connect_error) {
            throw new Exception('Connection failed: ' . $destinationDBConnection->connect_error);
        }

        // Escape the etlSource for safe SQL insertion
        $etlSource = $destinationDBConnection->real_escape_string($etlSource);

        // Prepare and execute the insert query
        $etlStartTStamp = time(); // Current Unix timestamp
        $query = "INSERT INTO PMSDATABASEmisc (schemaVersion, etlStartTStamp, etlInsertsCount, etlUpdatesCount, etlLogFile, etlSource) VALUES ($schemaVersion, $etlStartTStamp, $insertCount, $updateCount, '$errorLogFile', '$etlSource')";
        $result = $destinationDBConnection->query($query);

        // Check for query success
        if (!$result) {
            throw new Exception('Query failed: ' . $destinationDBConnection->error);
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully inserted tracking info into PMSDATABASEmisc for source: $etlSource" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the error with timestamp
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") insertEtlTrackingInfo failed: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Rethrow the exception for further handling
        throw $e;
    }
}



function updateEtlDuration($destDBConnection, $errorCount)
{
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        // Check destination connection
        if ($destDBConnection->connect_error) {
            throw new Exception('Connection failed to destination DB: ' . $destDBConnection->connect_error);
        }

        // Get the current Unix timestamp
        $currentTimestamp = time();

        // Get the most recent ETL start timestamp
        $queryStartTStamp = "SELECT etlStartTStamp FROM PMSDATABASEmisc ORDER BY id DESC LIMIT 1";
        $resultStartTStamp = $destDBConnection->query($queryStartTStamp);

        if (!$resultStartTStamp) {
            throw new Exception('Failed to fetch etlStartTStamp: ' . $destDBConnection->error);
        }

        $etlStartTStamp = $resultStartTStamp->fetch_assoc()['etlStartTStamp'] ?? 0;

        // Calculate the duration
        $etlDuration = $currentTimestamp - $etlStartTStamp;

        // Update the latest ETL record with the end timestamp and duration
        $updateQuery = "UPDATE PMSDATABASEmisc SET etlEndTStamp = $currentTimestamp, etlErrorsCount = $errorCount, etlDuration = $etlDuration ORDER BY id DESC LIMIT 1";
        $resultUpdate = $destDBConnection->query($updateQuery);

        if (!$resultUpdate) {
            throw new Exception('Failed to update ETL duration: ' . $destDBConnection->error);
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully updated ETL duration in PMSDATABASEmisc" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

    } catch (Exception $e) {

        // Log the error with timestamp
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") updateEtlDuration failed: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Rethrow the exception for further handling
        throw $e;
    }
}

function removeDuplicateRows($array, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        if (!is_array($array) || empty($array)) {
            throw new Exception("Invalid or empty array provided.");
        }

        // Serialize each array to a string
        $serializedArray = array_map('serialize', $array);

        // Remove duplicates
        $uniqueArray = array_unique($serializedArray);

        // Unserialize the unique array back to an array of arrays
        $uniqueArray = array_map('unserialize', $uniqueArray);

        // Re-index the array
        $result = array_values($uniqueArray);

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully removed duplicate rows" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $result;

    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;

        // Log the error
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in removeDuplicateRows: " . $e->getMessage() . PHP_EOL;
        error_log($errorMessage, 3, $errorLogFile);

        // Optionally rethrow or handle the exception further as needed
        // throw $e;

        return []; // Return an empty array or handle as per your application's error handling policy
    }
}



function getFirstNonNullImportCode($originDBConnection, $tableName, &$errorCount)
{
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
    $importCode = '';

    try {
        // Prepare and execute the query
        $importCodeQuery = "SELECT import_code FROM $tableName WHERE import_code IS NOT NULL LIMIT 1";
        $importCodeResult = $originDBConnection->query($importCodeQuery);

        // Check for query success
        if (!$importCodeResult) {
            throw new Exception('Query failed: ' . $originDBConnection->error);
        }

        // Fetch the result
        if ($row = $importCodeResult->fetch_assoc()) {
            $importCode = $row['import_code'];
        } else {
            // Handle the case when no non-null import_code is found
            throw new Exception('No non-null import_code found in ' . $tableName);
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully retrieved first non-null import_code from $tableName" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the error with timestamp
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") getFirstNonNullImportCode failed: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
    }

    return $importCode;
}




////Function to create and populate the ReservationlibProperty Table Associative Array
function createReservationLibProperty($reservations, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $reservationLibProperty[] = [
            'chainCode' => 'UNKNOWN',
            'propertyCode' => 'UNKNOWN',
            'dataSource' => 'HAPI',
        ];
        $seenCodes = [];
        $uniqueChainCodes = [];
        $uniquePropertyCodes = [];




        foreach ($reservations as $reservation) {
            // Validate if necessary fields exist in $reservation
            if (!isset($reservation['extracted_property_code']) || !isset($reservation['extracted_chain_code'])) {
                throw new Exception("Necessary fields missing in the reservation data.");
            }

            $propertyCode = $reservation['extracted_property_code'];
            $chainCode = $reservation['extracted_chain_code'];

            // Store unique propertyCode and chainCode
            if ($propertyCode !== 'UNKNOWN') {
                $uniquePropertyCodes[$propertyCode] = true;
            }
            if ($chainCode !== 'UNKNOWN') {
                $uniqueChainCodes[$chainCode] = true;
            }

            // Check for duplicate rows
            $codeCombo = $propertyCode . $chainCode;
            if (in_array($codeCombo, $seenCodes)) {
                continue;
            }
            $seenCodes[] = $codeCombo;

            // Create and populate the new associative array
            $newRecord = [
                'propertyCode' => $propertyCode,
                'chainCode' => $chainCode,
                'dataSource' => 'HAPI',
            ];

            $reservationLibProperty[] = $newRecord;
        }

        // Add UNKNOWN propertyCode for each unique chainCode
        foreach (array_keys($uniqueChainCodes) as $chainCode) {
            $reservationLibProperty[] = [
                'propertyCode' => 'UNKNOWN',
                'chainCode' => $chainCode,
                'dataSource' => 'HAPI',
            ];
        }

        // Add UNKNOWN chainCode for each unique propertyCode
        foreach (array_keys($uniquePropertyCodes) as $propertyCode) {
            $reservationLibProperty[] = [
                'propertyCode' => $propertyCode,
                'chainCode' => 'UNKNOWN',
                'dataSource' => 'HAPI',
            ];
        }

        // Remove duplicates from the array
        $reservationLibProperty = array_map("unserialize", array_unique(array_map("serialize", $reservationLibProperty)));

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed reservationLibProperty" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $reservationLibProperty;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Handle the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createReservationLibProperty: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if you need further handling outside this function
        throw $e;
    }
}




////Function to create and populate the ReservationlibSource Table Associative Array
function createReservationLibSource($data, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        if (empty($data)) {
            throw new Exception("Invalid data array.");
        }

        $result[] = [
            'RESERVATIONlibsource' => [
                'sourceName' => 'UNKNOWN',
                'sourceType' => 'UNKNOWN',
                'dataSource' => 'HAPI',
            ],
        ];
        $uniqueSourceNames = [];
        $uniqueSourceTypes = [];

        // Process each profile in the data
        foreach ($data as $profile) {
            if (!empty($profile['profiles'])) {
                $profiles = json_decode($profile['profiles'], true);

                foreach ($profiles as $profileData) {
                    $sourceName = $profileData['names'][0]['name'] ?? "";
                    $sourceType = $profileData['type'] ?? "";

                    // Store unique sourceName and sourceType
                    if ($sourceName !== 'UNKNOWN') {
                        $uniqueSourceNames[$sourceName] = true;
                    }
                    if ($sourceType !== 'UNKNOWN') {
                        $uniqueSourceTypes[$sourceType] = true;
                    }

                    $reservationLibSource = [
                        'RESERVATIONlibsource' => [
                            'sourceName' => $sourceName,
                            'sourceType' => $sourceType,
                            'dataSource' => 'HAPI',
                        ],
                    ];

                    // Check for duplicate rows
                    if (!in_array($reservationLibSource, $result)) {
                        $result[] = $reservationLibSource;
                    }
                }
            }
        }

        // Add UNKNOWN sourceType for each unique sourceName
        foreach (array_keys($uniqueSourceNames) as $sourceName) {
            $result[] = [
                'RESERVATIONlibsource' => [
                    'sourceName' => $sourceName,
                    'sourceType' => 'UNKNOWN',
                    'dataSource' => 'HAPI',
                ],
            ];
        }

        // Add UNKNOWN sourceName for each unique sourceType
        foreach (array_keys($uniqueSourceTypes) as $sourceType) {
            $result[] = [
                'RESERVATIONlibsource' => [
                    'sourceName' => 'UNKNOWN',
                    'sourceType' => $sourceType,
                    'dataSource' => 'HAPI',
                ],
            ];
        }

        // Remove duplicates from the array
        $result = array_map("unserialize", array_unique(array_map("serialize", $result)));

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed reservationLibSource" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $result;

    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the error
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createReservationLibSource: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception for further handling
        throw $e;
    }
}




////Function to create and populate the ReservationlibRoomType Table Associative Array
function createReservationLibRoomType($data, &$errorCount) {
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    try {
        if (empty($data)) {
            throw new Exception("Invalid data array.");
        }


        // Add initial row with Unknown values
        $result[] = [
            'ReservationLibRoomType' => [
                'typeName' => 'UNKNOWN',
                'typeCode' => 'UNKNOWN',
                'dataSource' => 'HAPI',
            ],
        ];

        foreach ($data as $reservation) {
            if (!empty($reservation['occupiedUnits'])) {
                $occupiedUnits = json_decode($reservation['occupiedUnits'], true);

                foreach ($occupiedUnits as $unit) {
                    $unitTypeCode = $unit['unitTypeCode'] ?? 'UNKNOWN';

                    $reservationLibRoomType = [
                        'ReservationLibRoomType' => [
                            'typeName' => 'UNKNOWN',
                            'typeCode' => $unitTypeCode,
                            'dataSource' => 'HAPI',
                        ],
                        // Additional fields can be added here
                    ];

                    // Check for duplicate rows
                    if (!in_array($reservationLibRoomType, $result)) {
                        $result[] = $reservationLibRoomType;
                    }
                }
            }
        }

        return $result;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Handle the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createReservationLibRoomType: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if you need further handling outside this function
        throw $e;
    }
}

////Function to create and populate the ReservationLibStayStatus Table Associative Array
function createReservationLibStayStatus($data, &$errorCount) {
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    try {
        if (empty($data)) {
            throw new Exception("Invalid data array.");
        }


        // Add 'Unknown' row at the beginning
        $unknownRow = [
            'RESERVATIONlibstaystatus' => [
                'statusName' => 'UNKNOWN',
                'dataSource' => 'HAPI',
            ],
        ];

        $result[] = $unknownRow;

        foreach ($data as $reservation) {
            $extStatus = $reservation['ext_status'] ?? 'UNKNOWN';
            $reservationLibStayStatus = [
                'RESERVATIONlibstaystatus' => [
                    'statusName' => $extStatus,
                    'dataSource' => 'HAPI',
                ],
                // Additional fields can be added here
            ];

            // Check for duplicate rows
            if (!in_array($reservationLibStayStatus, $result)) {
                $result[] = $reservationLibStayStatus;
            }
        }

        return $result;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Handle the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createReservationLibStayStatus: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if you need further handling outside this function
        throw $e;
    }
}



////Function to create and populate the ReservationGroup Table Associative Array

function createReservationGroup($data, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        if (empty($data)) {
            throw new Exception("Invalid data array.");
        }


        // Add 'Unknown' row at the beginning
        $unknownRow = [
            'RESERVATIONgroup' => [
                'groupName' => 'UNKNOWN',
                'groupNumber' => 'UNKNOWN',
                'groupStartDate' => '',
                'groupEndDate' => '',
                'dataSource' => 'HAPI',
            ],
        ];
        $result[] = $unknownRow;

        foreach ($data as $reservation) {
            // Extract relevant data for RESERVATIONgroup mapping
            $groupName = $reservation['groupName'] ?? 'UNKNOWN';
            $groupNumber = $reservation['groupNumber'] ?? 'UNKNOWN';
            $groupStartDate = $reservation['groupStartDate'] ?? '';
            $groupEndDate = $reservation['groupEndDate'] ?? '';

            $reservationGroup = [
                'RESERVATIONgroup' => [
                    'groupName' => $groupName,
                    'groupNumber' => $groupNumber,
                    'groupStartDate' => $groupStartDate,
                    'groupEndDate' => $groupEndDate,
                    'dataSource' => 'HAPI',
                ],
                // Additional fields can be added here
            ];

            // Check for duplicate rows
            if (!in_array($reservationGroup, $result)) {
                $result[] = $reservationGroup;
            }
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed ReservationGroup" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $result;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Handle the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createReservationGroup: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if needed
        throw $e;
    }
}



////Function to create and populate the ReservationLibRoomClass Table Associative Array
/**
 * @throws Exception
 */
function createReservationLibRoomClass($data, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        if (empty($data)) {
            throw new Exception("Invalid data array.");
        }

        $result = [];

        // Add 'Unknown' row at the beginning
        $unknownRow = [
            'RESERVATIONlibRoomClass' => [
                'className' => 'UNKNOWN',
                'dataSource' => 'HAPI',
            ],
        ];

        $result[] = $unknownRow;

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed ReservationLibRoomClass" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $result;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Handle the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createReservationLibRoomClass: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if needed
        throw $e;
    }
}


////Function to create and populate the ReservationLibRoom Table Associative Array
function createReservationLibRoom($array, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $reservationRooms = [];
        $uniqueCheck = []; // Array to keep track of existing rooms to prevent duplicates

        foreach ($array as $item) {
            // Initialize the room data with default values
            $roomData = [
                'roomNumber' => 'UNKNOWN',
                'dataSource' => 'HAPI' // Data source is always 'HAPI'
            ];

            // Extract room number from occupiedUnits
            if (isset($item['occupiedUnits']) && $item['occupiedUnits'] !== null) {
                $occupiedUnits = json_decode($item['occupiedUnits'], true);

                if (json_last_error() !== JSON_ERROR_NONE) {
                    // Log an error if JSON is not valid
                    $errorTimestamp = date('Y-m-d H:i:s');
                    $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") JSON decode error in occupiedUnits: " . json_last_error_msg() . PHP_EOL;
                    error_log($errorLogMessage, 3, $errorLogFile);
                    continue;
                }

                // Assuming the first unitId in the occupiedUnits is the room number
                if (!empty($occupiedUnits) && isset($occupiedUnits[0]['unitId'])) {
                    $roomData['roomNumber'] = $occupiedUnits[0]['unitId'];
                }
            }

            // Generate a unique key for the room to avoid duplicates
            $uniqueId = $roomData['roomNumber'];
            if (!isset($uniqueCheck[$uniqueId])) {
                $reservationRooms[] = $roomData;
                $uniqueCheck[$uniqueId] = true;
            }
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed ReservationLibRoom" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $reservationRooms;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createReservationLibRoom: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}



////Function to create and populate the CUSTOMERLibLoyaltyProgram Table Associative Array
function createCUSTOMERloyaltyProgram(&$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $loyaltyProgramArray = [
            [
                'CUSTOMERlibLoyaltyProgram' => [
                    'Name' => 'UNKNOWN',
                    'Source' => 'UNKNOWN',
                    'metaData' => [],
                    'dataSource' => 'HAPI'
                ]
            ],
            [
                'CUSTOMERlibLoyaltyProgram' => [
                    'Name' => 'Fairmont',
                    'Source' => 'Fairmont Banff Springs',
                    'metaData' => [],
                    'dataSource' => 'HAPI'
                ]
            ]
        ];

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully created CUSTOMER loyalty program array" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $loyaltyProgramArray;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createCUSTOMERloyaltyProgram: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}


function createCUSTOMERContactType(&$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $contactTypeArray = [
            [
                'CUSTOMERlibContactType' => [
                    'type' => 'UNKNOWN',
                    'dataSource' => 'HAPI'
                ]
            ],
            [
                'CUSTOMERlibContactType' => [
                    'type' => 'GUEST',
                    'dataSource' => 'HAPI'
                ]
            ],
            [
                'CUSTOMERlibContactType' => [
                    'type' => 'CUSTOMER',
                    'dataSource' => 'HAPI'
                ]
            ],
            [
                'CUSTOMERlibContactType' => [
                    'type' => 'PATRON',
                    'dataSource' => 'HAPI'
                ]
            ],
        ];

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully created CUSTOMER contact type array" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $contactTypeArray;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createCUSTOMERContactType: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}




////Function to create and populate the SERVICESLibTender Table Associative Array
function createSERVICESLibTender($data, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $result[] = [
            'paymentMethod' => 'UNKNOWN',
            'dataSource' => 'HAPI',
        ];

        $uniqueKeys = []; // Array to keep track of unique combinations of paymentMethod and dataSource

        foreach ($data as $record) {
            $paymentMethod = $record['paymentMethod'] ?? null;

            if ($paymentMethod) {
                $code = substr($paymentMethod, 10, 2); // Extract the first 2 letters
                $hash = md5($code . 'HAPI'); // Create a unique hash based on paymentMethod and dataSource

                // Only add to result if this combination hasn't been added before
                if (!isset($uniqueKeys[$hash])) {
                    $uniqueKeys[$hash] = true;
                    $result[] = [
                        'paymentMethod' => $code,
                        'dataSource' => 'HAPI',
                    ];
                }
            }
        }

        // No need to call removeDuplicateRows2D, as duplicates have been filtered in the loop

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed SERVICES LibTender" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $result;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;

        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createSERVICESLibTender: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}



////Function to create and populate the SERVICESlibServiceItems Table Associative Array
function createSERVICESlibserviceitems($array, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $serviceItems = [];
        $uniqueCheck = []; // Array to keep track of existing items to prevent duplicates

        foreach ($array as $item) {
            $ratePlans = isset($item['ratePlans']) ? json_decode($item['ratePlans'], true) : [];
            $prices = isset($item['prices']) ? json_decode($item['prices'], true) : [];

            if (json_last_error() !== JSON_ERROR_NONE) {
                // Log an error if JSON decoding fails
                $errorTimestamp = date('Y-m-d H:i:s');
                $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") JSON decode error: " . json_last_error_msg() . PHP_EOL;
                error_log($errorLogMessage, 3, $errorLogFile);
                continue;
            }

            // Initialize service item data with default values
            $serviceItemData = [
                'itemName' => '', // As per mapping, itemName remains empty
                'itemCode' => '',
                'ratePlanCode' => '',
                'dataSource' => 'HAPI' // Data source is always 'HAPI'
            ];

            // Extract rate plan code from Prices
            if (!empty($prices) && isset($prices[0]['ratePlanCode'])) {
                $serviceItemData['ratePlanCode'] = $prices[0]['ratePlanCode'];
            }

            // Extract item code from Services
            if (!empty($ratePlans) && isset($ratePlans[0]['code'])) {
                $serviceItemData['itemCode'] = $ratePlans[0]['code'];
            }

            // Create a unique key to check for duplicates
            $uniqueKey = $serviceItemData['itemCode'] . '|' . $serviceItemData['ratePlanCode'];
            if (!isset($uniqueCheck[$uniqueKey])) {
                $serviceItems[] = $serviceItemData; // Original record
                $uniqueCheck[$uniqueKey] = true; // Mark this key as seen

                // Duplicate the record with 'UNKNOWN' itemCode if ratePlanCode is not 'UNKNOWN'
                if ($serviceItemData['ratePlanCode'] !== 'UNKNOWN') {
                    $duplicateItem = $serviceItemData;
                    $duplicateItem['itemCode'] = 'UNKNOWN';
                    $serviceItems[] = $duplicateItem; // Duplicated record
                }
            }
        }

        // Add the 'UNKNOWN' record at the beginning of the array
        array_unshift($serviceItems, [
            'itemName' => 'UNKNOWN',
            'itemCode' => 'UNKNOWN',
            'ratePlanCode' => 'UNKNOWN',
            'dataSource' => 'HAPI'
        ]);

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed SERVICES LibServiceItems" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $serviceItems;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createSERVICESlibserviceitems: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}



//////Function to create and populate the SERVICESlibFolioOrderType Table Associative Array
//function createSERVICESlibFolioOrderType(&$errorCount) {
//    // Define the error log file path
//    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
//
//    try {
//        $FolioOrderTypeArray = [
//            [
//                'SERVICESlibFolioOrdersType' => [
//                    'orderType' => 'UNKNOWN',
//                    'dataSource' => 'HAPI'
//                ]
//            ],
//            [
//                'SERVICESlibFolioOrdersType' => [
//                    'orderType' => 'RESERVATION',
//                    'dataSource' => 'HAPI'
//                ]
//            ],
//            [
//                'SERVICESlibFolioOrdersType' => [
//                    'orderType' => 'SERVICE',
//                    'dataSource' => 'HAPI'
//                ]
//            ],
//            [
//                'SERVICESlibFolioOrdersType' => [
//                    'orderType' => 'OTHERS',
//                    'dataSource' => 'HAPI'
//                ]
//            ],
//        ];
//
//        // Log the success message
//        $errorTimestamp = date('Y-m-d H:i:s');
//        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed SERVICES lib Folio Order Type array" . PHP_EOL;
//        error_log($successMessage, 3, $errorLogFile);
//
//        return $FolioOrderTypeArray;
//    } catch (Exception $e) {
//        // Increment error counter
//        $errorCount++;
//        // Log the exception
//        $errorTimestamp = date('Y-m-d H:i:s');
//        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createSERVICESlibFolioOrderType: " . $e->getMessage() . PHP_EOL;
//        error_log($errorLogMessage, 3, $errorLogFile);
//
//        // Optionally rethrow the exception if further handling is required
//        throw $e;
//    }
//}



function createArrCUSTOMERcontact($normalizedData, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $customerContacts = [
            [ // Add a 'UNKNOWN' record at the beginning of the array
                'firstName' => 'UNKNOWN',
                'lastName' => 'UNKNOWN',
                'title' => 'UNKNOWN',
                'email' => 'UNKNOWN',
                'languageCode' => 'UNKNOWN',
                'languageFormat' => 'UNKNOWN',
                'extGuestId' => 'UNKNOWN',
                'dataSource' => 'HAPI'
            ]
        ];

        $uniqueCheck = []; // Array to keep track of existing contacts to prevent duplicates


        foreach ($normalizedData as $item) {
            if (isset($item['guests']) && !empty($item['guests'])) {
                foreach ($item['guests'] as $guest) {
                    foreach ($item['guests'] as $guest_ind) {
                        if (!isset($guest_ind['guest']) || !isset($guest_ind['guest']['names'][0])) {
                            continue;
                        }
                        $guestData = $guest_ind['guest'];
                        $nameData = $guestData['names'][0];
                        //metaData
                        $metaDataArray = [
                            'addresses' => $guest_ind['addresses'] ?? null,
                            'createdBy' => $guest_ind['createdBy'] ?? null,
                            'createdDateTime' => $guest_ind['createdDateTime_repo'] ?? null,
                            'guest' => $guest_ind['guest'],
                            'contactDetails' => $guest_ind['contactDetails']

                        ];
                        $metaDataJson = json_encode($metaDataArray);
                        // Initialize email as empty string
                        $email = '';

                        // Iterate through contact details to find email
                        if (isset($guestData['contactDetails']) && is_array($guestData['contactDetails'])) {
                            foreach ($guestData['contactDetails'] as $contactDetail) {
                                if ($contactDetail['category'] === 'EMAIL' && isset($contactDetail['value'])) {
                                    $email = $contactDetail['value'];
                                    break; // Stop the loop once email is found
                                }
                            }
                        }


                        $contact = [
                            'firstName' => $nameData['givenName'] ?? null,
                            'lastName' => $nameData['surname'] ?? null,
                            'title' => $nameData['title'] ?? null,
                            'email' => $email ?? '', // Use the extracted email
                            'birthDate' => $guestData['dateOfBirth'] ?? null,
                            'languageCode' => $guestData['primaryLanguage']['code'] ?? null,
                            'languageFormat' => $guestData['primaryLanguage']['format'] ?? null,
                            'extGuestId' => $item['extracted_guest_id'] ?? null,
                            'isPrimary' => $guest['isPrimary'] ?? null,
                            'metaData' => $metaDataJson ?? null,
                            'dataSource' => 'HAPI'
                        ];

                        $uniqueId = $contact['firstName'] . '|' . $contact['lastName'] . '|' . $contact['extGuestId'];
                        if (!isset($uniqueCheck[$uniqueId])) {
                            $customerContacts[] = $contact;
                            $uniqueCheck[$uniqueId] = true;
                        }
                    }
                }
            }
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed CUSTOMER contacts" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $customerContacts;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createCUSTOMERcontact: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}

//function createArrCUSTOMERcontact($normalizedData, &$errorCount) {
//    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
//
//    try {
//        $customerContacts = [
//            [
//                'firstName' => 'UNKNOWN',
//                'lastName' => 'UNKNOWN',
//                'title' => 'UNKNOWN',
//                'email' => 'UNKNOWN',
//                'languageCode' => 'UNKNOWN',
//                'languageFormat' => 'UNKNOWN',
//                'extGuestId' => 'UNKNOWN',
//                'dataSource' => 'HAPI'
//            ]
//        ];
//
//        $uniqueCheck = [];
//
//        foreach ($normalizedData as $item) {
//            $firstGuestAttributes = []; // To store the first guest's email, phone, and address
//
//            if (isset($item['guests']) && !empty($item['guests'])) {
//                foreach ($item['guests'] as $index => $guest_ind) {
//                    if (!isset($guest_ind['guest']) || !isset($guest_ind['guest']['names'][0])) {
//                        continue;
//                    }
//                    $guestData = $guest_ind['guest'];
//                    $nameData = $guestData['names'][0];
//
//                    $metaDataArray = [
//                        'addresses' => $guest_ind['addresses'] ?? null,
//                        'createdBy' => $guest_ind['createdBy'] ?? null,
//                        'createdDateTime' => $guest_ind['createdDateTime_repo'] ?? null,
//                        'guest' => $guest_ind['guest'],
//                        'contactDetails' => $guest_ind['contactDetails']
//                    ];
//                    $metaDataJson = json_encode($metaDataArray);
//
//                    $email = '';
//                    $phone = '';
//                    $address = '';
//
//                    if ($index === 0 || !isset($guestData['contactDetails']) || empty($guestData['contactDetails'])) {
//                        // Process contact details for the first guest or if subsequent guests lack details
//                        if (isset($guestData['contactDetails']) && is_array($guestData['contactDetails'])) {
//                            foreach ($guestData['contactDetails'] as $contactDetail) {
//                                if ($contactDetail['category'] === 'EMAIL' && isset($contactDetail['value'])) {
//                                    $email = $contactDetail['value'];
//                                } elseif ($contactDetail['category'] === 'PHONE' && isset($contactDetail['value'])) {
//                                    $phone = $contactDetail['value'];
//                                }
//                            }
//                        }
//                        if (isset($guestData['addresses']) && is_array($guestData['addresses']) && !empty($guestData['addresses'][0])) {
//                            $address = $guestData['addresses'][0]; // Assuming first address is the primary one
//                        }
//
//                        if ($index === 0) {
//                            $firstGuestAttributes = ['email' => $email, 'phone' => $phone, 'address' => $address];
//                        }
//                    } else {
//                        // For subsequent guests without their own details, use the first guest's details
//                        $email = $firstGuestAttributes['email'] ?? '';
//                        $phone = $firstGuestAttributes['phone'] ?? '';
//                        $address = $firstGuestAttributes['address'] ?? '';
//                    }
//
//                    $contact = [
//                        'firstName' => $nameData['givenName'] ?? null,
//                        'lastName' => $nameData['surname'] ?? null,
//                        'title' => $nameData['title'] ?? null,
//                        'email' => $email, // Use the determined email
//                        'phone' => $phone, // Use the determined phone
//                        'address' => $address, // Use the determined address
//                        'birthDate' => $guestData['dateOfBirth'] ?? null,
//                        'languageCode' => $guestData['primaryLanguage']['code'] ?? null,
//                        'languageFormat' => $guestData['primaryLanguage']['format'] ?? null,
//                        'extGuestId' => $item['extracted_guest_id'] ?? null,
//                        'isPrimary' => $guest_ind['isPrimary'] ?? null,
//                        'metaData' => $metaDataJson ?? null,
//                        'dataSource' => 'HAPI'
//                    ];
//
//                    $uniqueId = $contact['firstName'] . '|' . $contact['lastName'] . '|' . $contact['extGuestId'];
//                    if (!isset($uniqueCheck[$uniqueId])) {
//                        $customerContacts[] = $contact;
//                        $uniqueCheck[$uniqueId] = true;
//                    }
//                }
//            }
//        }
//
//        $errorTimestamp = date('Y-m-d H:i:s');
//        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed CUSTOMER contacts" . PHP_EOL;
//        error_log($successMessage, 3, $errorLogFile);
//
//        return $customerContacts;
//    } catch (Exception $e) {
//        $errorCount++;
//        $errorTimestamp = date('Y-m-d H:i:s');
//        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createCUSTOMERcontact: " . $e->getMessage() . PHP_EOL;
//        error_log($errorLogMessage, 3, $errorLogFile);
//
//        throw $e;
//    }
//}


function createArrRESERVATIONstay(
    $connection,
    $normalizedData,
    $arrRESERVATIONlibSource,
    $arrRESERVATIONlibProperty,
    &$errorCount
) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $arrRESERVATIONStay = [];

        // Create lookups directly within the function
        $sourceLookup = [];
        $propertyLookup = [];


        // Fetch and populate sourceLookup
        $sourceQuery = "SELECT id, sourceName, sourceType FROM RESERVATIONlibSource";
        $sourceResult = $connection->query($sourceQuery);
        if (!$sourceResult) {
            throw new Exception("Query failed for source lookup: " . $connection->error);
        }
        while ($row = $sourceResult->fetch_assoc()) {
            $sourceName = $row['sourceName'] ?? 'UNKNOWN';
            $sourceType = $row['sourceType'] ?? 'UNKNOWN';
            $sourceLookup[$sourceName][$sourceType] = $row['id'];
        }
        $sourceResult->free();

        // Fetch and populate propertyLookup
        $propertyQuery = "SELECT id, propertyCode, chainCode FROM RESERVATIONlibProperty";
        $propertyResult = $connection->query($propertyQuery);
        if (!$propertyResult) {
            throw new Exception("Query failed for property lookup: " . $connection->error);
        }
        while ($row = $propertyResult->fetch_assoc()) {
            $propertyCode = $row['propertyCode'] ?? 'UNKNOWN';
            $chainCode = $row['chainCode'] ?? 'UNKNOWN';
            $propertyLookup[$propertyCode][$chainCode] = $row['id'];
        }
        $propertyResult->free();

        foreach ($normalizedData as $entry) {
            //check if confirmation number is null, skip if null
            if (is_null($entry['confirmation_number'])) {
                continue;
            };
            // Previously handled fields
            $createDateTime = $entry['createdDateTime'] ?? null;
            $modifyDateTime = $entry['lastModifiedDateTime'] ?? null;

            //metaData
            $metaDataArray = [
                'created_by' => $entry['createdBy'] ?? null,
                'created_datetime' => $entry['createdDateTime'] ?? null,
                'created_datetime_repo' => $entry['createdDateTime_repo'] ?? null,
                'departure' => $entry['departure'],
                'doNotDisplayPrice' => $entry['doNotDisplayPrice'],
                'estimatedDateTimeOfArrival' => $entry['estimatedDateTimeOfArrival'],
                'estimatedDateTimeOfDeparture' => $entry['estimatedDateTimeOfDeparture'],
                'ext_id' => $entry['ext_id'],
                'ext_status' => $entry['ext_status'],
                'confirmation_number' => $entry['confirmation_number'],
                'reservation_id' => $entry['reservation_id'],
                'referenceIds' => $entry['referenceIds'],
                'paymentMethod' => $entry['paymentMethod'],
                'prices' => $entry['prices'],
                'processtStamp' => $entry['procesststamp'],
                'promotionCode' => $entry['promotionCode'],
                'purposeOfStay' => $entry['purposeOfStay'],
                'ratePlans' => $entry['ratePlans'],
                'receivedDateTime' => $entry['receivedDateTime'],
                'command_id' => $entry['command_id'],
                'additionalData' => $entry['additionalData'],
                'forecastRevenue' => $entry['forecastRevenue'],
                'comments' => $entry['comments'],
                'sharerIds' => $entry['sharerIds'],
                'requestedDeposits' => $entry['requestedDeposits'],
                'alerts' => $entry['alerts'],
                'contacts' => $entry['contacts'],
                'segmentations' => $entry['segmentations']
            ];
            $metaDataJson = json_encode($metaDataArray);

            // New fields
            $departureDate = $entry['departure'] ?? null;
            $arrivalDate = $entry['arrival'] ?? null;
            $createdBy = $entry['createdBy'] ?? null;
            $extPMSConfNum = $entry['confirmation_number'] ?? null;
            $extReservationId = $entry['reservation_id'] ?? null;
            $extGuestId = $entry['extracted_guest_id'] ?? '';
            $propertyCode = $entry['extracted_property_code'] ?? 'UNKNOWN';
            $chainCode = $entry['extracted_chain_code'] ?? 'UNKNOWN';
            $sourceName = $entry['profiles'][0]['names'][0]['name'] ?? 'UNKNOWN';
            $sourceType = $entry['profiles'][0]['type'] ?? 'UNKNOWN';

            $dataSource = 'HAPI'; // assuming it's a constant value

            // Utilize the lookups for libSourceId and libPropertyId
            $libSourceId = $sourceLookup[$sourceName][$sourceType] ?? null;
            $libPropertyId = $propertyLookup[$propertyCode][$chainCode] ?? null;

            $arrRESERVATIONStay[] = [
                'createDateTime' => strtotime($createDateTime),
                'modifyDateTime' => strtotime($modifyDateTime),
                'startDate' => $arrivalDate,
                'endDate' => $departureDate,
                'createdBy' => $createdBy,
                'metaData' => $metaDataJson, // Placeholder for future metadata inclusion
                'extPMSConfNum' => $extPMSConfNum,
                'extReservationId' => $extReservationId,
                'extGuestId' => $extGuestId,
                'dataSource' => $dataSource,
                'libSourceId' => $libSourceId,
                'libPropertyId' => $libPropertyId,
                'propertyCode' => $propertyCode,
                'chainCode' => $chainCode,
                'sourceName' => $sourceName,
                'sourceType' => $sourceType,
            ];
        }



        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed ARR RESERVATION stay data" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $arrRESERVATIONStay;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createArrRESERVATIONstay: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}



//take table from relational database and convert it into an associative array. Using this method to get around how the
//Id field is generated table-side and not easily predictable since it's generated at the server level instead of table
//level
function getTableAsAssociativeArray($connection, $tableName) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        // Ensure the table name is safe to use in a query
        $tableName = mysqli_real_escape_string($connection, $tableName);

        // Query to get all rows from the table
        $query = "SELECT * FROM `$tableName`";
        $result = mysqli_query($connection, $query);

        // Check for a valid result
        if (!$result) {
            throw new Exception('Query failed: ' . mysqli_error($connection));
        }

        // Create an associative array to store table data
        $tableData = [];

        // Fetch each row as an associative array
        while ($row = mysqli_fetch_assoc($result)) {
            $tableData[] = $row;
        }

        // Free result set
        mysqli_free_result($result);

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully fetched data from `$tableName`" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $tableData;
    } catch (Exception $e) {
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in getTableAsAssociativeArray: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}






//create the arrCUSTOMERrelationship array by parsing from $myDataSemiParsed
function createArrCUSTOMERrelationship($myDataSemiParsed, $arrCUSTOMERlibContactType, $arrCUSTOMERcontact, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $arrCUSTOMERrelationship = [];

        // Create a lookup for contact types
        $contactTypeLookup = [];
        foreach ($arrCUSTOMERlibContactType as $type) {
            $contactTypeLookup[$type['type']] = $type['id'];
        }

        // Create a lookup for contacts
        $contactLookup = [];
        foreach ($arrCUSTOMERcontact as $contact) {
            $key = strtolower($contact['firstName'] . $contact['lastName'] . $contact['extGuestId']); // using lower case for case-insensitive comparison
            $contactLookup[$key] = $contact['id'];
        }

        foreach ($myDataSemiParsed as $entry) {


            $guestsData = json_decode($entry['guests'], true) ?? [];
            foreach ($guestsData as $guestData) {
                $guestInfo = $guestData['guest'] ?? null;
                if ($guestInfo) {
//                    $isPrimary = isset($guestData['isPrimary']) ? (int)$guestData['isPrimary'] : 0;
                    $firstName = $guestInfo['names'][0]['givenName'] ?? null;
                    $lastName = $guestInfo['names'][0]['surname'] ?? null;
                    $extGuestId = $entry['extracted_guest_id'] ?? '';

                    $contactKey = strtolower($firstName . $lastName . $extGuestId);

                    $contactTypeId = $contactTypeLookup['GUEST'] ?? null;
                    $contactId = $contactLookup[$contactKey] ?? null;

                    if ($contactTypeId !== null && $contactId !== null) {
                        $arrCUSTOMERrelationship[] = [
//                            'isPrimary' => $isPrimary,
                            'contactTypeId' => $contactTypeId,
                            'type' => 'GUEST',
                            'contactId' => $contactId,
                            'firstName' => $firstName,
                            'LastName' => $lastName,
                            'extGuestId' => $extGuestId,
                            'dataSource' => 'HAPI'
                        ];
                    }
                }
            }
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed CUSTOMER relationship data" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $arrCUSTOMERrelationship;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createArrCUSTOMERrelationship: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}




function createLookup($mysqli, $tableName, $keyField1, $keyField2, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $lookup = [];
        $query = "SELECT id, $keyField1, $keyField2 FROM $tableName";

        $result = $mysqli->query($query);
        if (!$result) {
            throw new Exception("Query failed: " . $mysqli->error);
        }

        while ($row = $result->fetch_assoc()) {
            $key1 = $row[$keyField1] ?? 'UNKNOWN';
            $key2 = $row[$keyField2] ?? 'UNKNOWN';
            $lookup[$key1][$key2] = $row['id'];
        }
        $result->free();

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully created lookup for $tableName" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $lookup;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createLookup for $tableName: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}







//create the arrCUSTOMERmembership array by parsing from $myDataSemiParsed
function createArrCUSTOMERmembership($myDataSemiParsed, $arrCUSTOMERlibLoyaltyProgram, $arrCUSTOMERcontact, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $arrCUSTOMERmembership = [];
        $defaultContactId = array_column($arrCUSTOMERcontact, 'id', 'extGuestId')['UNKNOWN'];
        $loyaltyProgramIdForFairmont = null;

        // Find the ID for 'Fairmont Banff Springs' from arrCUSTOMERlibLoyaltyProgram
        foreach ($arrCUSTOMERlibLoyaltyProgram as $program) {
            if ($program['name'] === 'Fairmont' && $program['source'] === 'Fairmont Banff Springs') {
                $loyaltyProgramIdForFairmont = $program['id'];
                break;
            }
        }

        foreach ($myDataSemiParsed as $entry) {
            $memberships = json_decode($entry['memberships'], true) ?? [];
            $guestData = json_decode($entry['guests'], true)[0]['guest'] ?? null;

            if (!empty($memberships) && $guestData) {
                $firstName = $guestData['names'][0]['givenName'] ?? null;
                $lastName = $guestData['names'][0]['surname'] ?? null;
                $extGuestId = $entry['extracted_guest_id'] ?? '';

                foreach ($memberships as $membership) {
                    if (!empty($membership['membershipCode'])) {
                        $libLoyaltyProgramId = $loyaltyProgramIdForFairmont;
                    } else {
                        $libLoyaltyProgramId = null;
                    }

                    $contactId = $defaultContactId;
                    foreach ($arrCUSTOMERcontact as $contact) {
                        if ($contact['extGuestId'] === $extGuestId && $contact['firstName'] === $firstName && $contact['lastName'] === $lastName) {
                            $contactId = $contact['id'];
                            break;
                        }
                    }

                    $arrCUSTOMERmembership[] = [
                        'level' => $membership['level'] ?? null,
                        'membershipCode' => $membership['membershipCode'] ?? null,
                        'dataSource' => 'HAPI',
                        'libLoyaltyProgramId' => $libLoyaltyProgramId,
                        'Name' => $membership['Name'] ?? null,
                        'Source' => $membership['Source'] ?? null,
                        'contactId' => $contactId,
                        'firstName' => $firstName,
                        'LastName' => $lastName,
                        'extGuestId' => $extGuestId,
                    ];
                }
            }
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed CUSTOMER membership data" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $arrCUSTOMERmembership;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createArrCUSTOMERmembership: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}





//create the arrSERVICESpayment array by parsing from $myDataSemiParsed
function createArrSERVICESpayment($myDataSemiParsed, $arrSERVICESlibTender, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $arrSERVICESpayment = [];

        // Create a lookup array for libTender based on paymentMethod
        $libTenderLookup = [];
        foreach ($arrSERVICESlibTender as $tender) {
            $libTenderLookup[$tender['paymentMethod']] = $tender['id'];
        }

        // Get the default id for UNKNOWN paymentMethod
        $defaultLibTenderId = $libTenderLookup['UNKNOWN'] ?? null;

        foreach ($myDataSemiParsed as $entry) {
            // Initialize default values
            $paymentAmount = 0; // Always null as specified
            $currencyCode = isset($entry['currency']) ? json_decode($entry['currency'], true)['code'] : 'UNKNOWN';
            $dataSource = 'HAPI'; // Constant value 'HAPI'
            $paymentMethod = isset($entry['paymentMethod']) ? json_decode($entry['paymentMethod'], true)['code'] : null;

            if (json_last_error() !== JSON_ERROR_NONE) {
                // Log an error if JSON decoding fails
                $errorTimestamp = date('Y-m-d H:i:s');
                $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") JSON decode error in createArrSERVICESpayment: " . json_last_error_msg() . PHP_EOL;
                error_log($errorLogMessage, 3, $errorLogFile);
                continue;
            }

            // Match libTenderId using the lookup array, default to UNKNOWN if not found
            $libTenderId = $libTenderLookup[$paymentMethod] ?? $defaultLibTenderId;

            // Construct the associative array
            $paymentData = [
                'paymentAmount' => $paymentAmount,
                'currencyCode' => $currencyCode,
                'dataSource' => $dataSource,
                'libTenderId' => $libTenderId,
                'paymentMethod' => $paymentMethod
            ];

            $arrSERVICESpayment[] = $paymentData;
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed SERVICES payment data" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $arrSERVICESpayment;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createArrSERVICESpayment: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}


//create the arrRESERVATIONgroupStay array by parsing from $myDataSemiParsed
function createArrRESERVATIONgroupStay($arrRESERVATIONstay, $arrRESERVATIONgroup, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $arrRESERVATIONgroupStay = [];

        // Preparing a mapping for quick lookup by the required fields
        $stayLookup = [];
        foreach ($arrRESERVATIONstay as $stay) {
            $lookupKey = $stay['startDate'] . '|' . $stay['endDate'] . '|' . $stay['extPMSConfNum'];
            $stayLookup[$lookupKey] = $stay['id'];
        }

        $groupLookup = [];
        foreach ($arrRESERVATIONgroup as $group) {
            $lookupKey = $group['groupName'] . '|' . $group['groupNumber'] . '|' . $group['groupStartDate'] . '|' . $group['groupEndDate'];
            $groupLookup[$lookupKey] = $group['id'];
        }

        foreach ($arrRESERVATIONstay as $item) {
            $groupStayData = [
                'HAPI' => $item['dataSource'],
                'stayId' => '', // To be populated from arrRESERVATIONstay
                'startDate' => $item['arrival'],
                'endDate' => $item['departure'],
                'extPMSConfNum' => $item['extPMSConfNum'],
                'groupId' => '', // To be populated from arrRESERVATIONgroup
                'groupName' => 'UNKNOWN',
                'groupNumber' => 'UNKNOWN',
                'groupStartDate' => null,
                'groupEndDate' => null
            ];

            // Look up stayId
            $stayKey = $item['startDate'] . '|' . $item['endDate'] . '|' . $item['extPMSConfNum'];
            if (isset($stayLookup[$stayKey])) {
                $groupStayData['stayId'] = $stayLookup[$stayKey];
            }

            // Look up groupId
            foreach ($arrRESERVATIONgroup as $group) {
                if (
                    $group['groupName'] === $groupStayData['groupName'] &&
                    $group['groupNumber'] === $groupStayData['groupNumber'] &&
                    $group['groupStartDate'] === $groupStayData['groupStartDate'] &&
                    $group['groupEndDate'] === $groupStayData['groupEndDate']
                ) {
                    $groupStayData['groupId'] = $group['id'];
                    break;
                }
            }

            // Add the group stay data to the result array
            $arrRESERVATIONgroupStay[] = $groupStayData;
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed RESERVATION group stay data" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $arrRESERVATIONgroupStay;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createArrRESERVATIONgroupStay: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}


function indexArrReservationStay($arrRESERVATIONstay) {
    $indexedStays = [];
    foreach ($arrRESERVATIONstay as $stay) {
        // Create a unique key for each stay
        $key = $stay['startDate'] . '|' . $stay['endDate'] . '|' . strtotime($stay['createDateTime']) . '|' . strtotime($stay['modifyDateTime']);
        $indexedStays[$key] = $stay['id'];
    }
    return $indexedStays;
}

function createArrRESERVATIONstayStatusStay($normalizedData, $arrRESERVATIONstay, $arrRESERVATIONlibStayStatus, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $arrRESERVATIONstayStatusStay = [];



        // Indexing reservation stays for fast lookup
        $indexedReservationStays = [];
        foreach ($arrRESERVATIONstay as $stay) {
            $index = $stay['extPMSConfNum'];
            $indexedReservationStays[$index] = $stay['id'];
        }

        foreach ($normalizedData as $entry) {
            if (is_null($entry['confirmation_number'])){
                continue;
            }
            $createDateTime = strval(strtotime($entry['createdDateTime'])) ?? null;
            $modifyDateTime = strval(strtotime($entry['lastModifiedDateTime'])) ?? null;

            $libStayStatusId = null;
            foreach ($arrRESERVATIONlibStayStatus as $status) {
                if ($status['statusName'] === ($entry['ext_status'] ?? 'UNKNOWN')) {
                    $libStayStatusId = $status['id'];
                    break;
                }
            }

            $startDate = $entry['arrival'] ?? null; // Assuming these are already in the correct format
            $endDate = $entry['departure'] ?? null;
            $extGuestId = $entry['extracted_guest_id'] ?? '';
            $extPMSConfNum = $entry['confirmation_number'] ?? null;

            // Create index for stay lookup
            $stayIndex = $extPMSConfNum;
            // Lookup for stayId using the index
            if (isset($indexedReservationStays[$stayIndex])) {
                $stayId = is_array($indexedReservationStays[$stayIndex])
                    ? reset($indexedReservationStays[$stayIndex])
                    : $indexedReservationStays[$stayIndex];
            } else {
                $stayId = null;
            }


            $arrRESERVATIONstayStatusStay[] = [
                'cancelledBy' => $entry['cancellationDetails']['cancelledBy'] ?? null,
                'cancellationDateTime' => $entry['cancellationDetails']['cancellationDateTime'] ?? null,
                'cancellationReasonCode' => $entry['cancellationDetails']['cancellationReasonCode'] ?? null,
                'cancellationReasonText' => $entry['Cancellation']['cancellationReasonText'] ?? null,
                'dataSource' => 'HAPI',
                'stayId' => $stayId,
                'createDateTime' => $createDateTime,
                'modifyDateTime' => $modifyDateTime,
                'startDate' => $startDate,
                'endDate' => $endDate,
                'extGuestId' => $extGuestId,
                'extPMSConfNum' => $extPMSConfNum,
                'libStayStatusId' => $libStayStatusId,
                'statusName' => $entry['ext_status'] ?? 'UNKNOWN'
            ];
        }

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed RESERVATION stay status data" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $arrRESERVATIONstayStatusStay;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createArrRESERVATIONstayStatusStay: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}



function createArrRESERVATIONroomDetails(
    $normalizedData,
    $arrCUSTOMERcontact,
    $arrRESERVATIONstay,
    $arrRESERVATIONlibRoomType,
    $arrRESERVATIONlibRoomClass,
    $arrRESERVATIONlibRoom,
    &$errorCount
) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    // Create indexed arrays for fast lookup
    $indexedContacts = [];
    foreach ($arrCUSTOMERcontact as $contact) {
        $index = $contact['firstName'] . '|' . $contact['lastName'] . '|' . $contact['extGuestId'];
        $indexedContacts[$index] = $contact['id'];
    }

    // Indexing reservation stays for fast lookup
    $indexedReservationStays = [];
    foreach ($arrRESERVATIONstay as $stay) {
        $index = $stay['extPMSConfNum'];
        $indexedReservationStays[$index] = $stay['id'];
    }

    $indexedRoomTypes = [];
    foreach ($arrRESERVATIONlibRoomType as $roomType) {
        $indexedRoomTypes[$roomType['typeCode']] = $roomType['id'];
    }

    // Find the default ID for 'UNKNOWN' room type
    $defaultLibRoomTypeId = null;
    foreach ($arrRESERVATIONlibRoomType as $roomType) {
        if ($roomType['typeCode'] === 'UNKNOWN' && $roomType['typeName'] === 'UNKNOWN') {
            $defaultLibRoomTypeId = $roomType['id'];
            break;
        }
    }

    $indexedRoomClasses = [];
    foreach ($arrRESERVATIONlibRoomClass as $roomClass) {
        $indexedRoomClasses[$roomClass['className']] = $roomClass['id'];
    }

    $indexedLibRooms = [];
    foreach ($arrRESERVATIONlibRoom as $room) {
        $indexedLibRooms[$room['roomNumber']] = $room['id'];
    }

    try {
        $arrRESERVATIONroomDetails = [];
        foreach ($normalizedData as $entry) {
            //skip where confirmation number is null
            if(is_null($entry['confirmation_number'])){
                continue;
            }

            // Existing logic to decode and extract guest details...

            // New logic to decode and extract prices and ratePlans
            $pricesData = $entry['prices'] ?? [];
            $ratePlansData = $entry['ratePlans'] ?? [];
            $amount = (!empty($pricesData) && isset($pricesData[0]['amount'])) ? $pricesData[0]['amount'] : null;
            $ratePlanCode = (!empty($ratePlansData) && isset($ratePlansData[0]['code'])) ? $ratePlansData[0]['code'] : null;

            // Decode and extract guest details
            $guestDetails = $entry['guests'][0]['guest'] ?? null;



            // Extract room number and typeCode from occupiedUnits
//            $occupiedUnits = $entry['occupiedUnits'] ??
            if (isset($entry['occupiedUnits'][0]['unitId'])) {
                $roomNumber = $entry['occupiedUnits'][0]['unitId'];
            } else {
                $roomNumber = 'UNKNOWN'; // Default value if unitId is not present
            }
            if (isset($entry['occupiedUnits'][0]['unitTypeCode'])) {
                $typeCode = $entry['occupiedUnits'][0]['unitTypeCode'];
            } else {
                $typeCode = 'UNKNOWN'; // Default value if unitId is not present
            }


            // Lookup libRoomId and libRoomTypeId based on roomNumber and typeCode

            $libRoomId = $indexedLibRooms[$roomNumber] ?? null;
            $libRoomTypeId = $indexedRoomTypes[$typeCode] ?? $defaultLibRoomTypeId;


            if ($guestDetails) {
                $givenName = $guestDetails['names'][0]['givenName'] ?? null;
                $surname = $guestDetails['names'][0]['surname'] ?? null;
                $contactIndex = $givenName . '|' . $surname . '|' . $entry['extracted_guest_id'];
                $contactId = $indexedContacts[$contactIndex] ?? null;
                $startDate = $entry['arrival'] ?? null; // Assuming these are already in the correct format
                $endDate = $entry['departure'] ?? null;
                $createDateTime = strtotime($entry['createdDateTime']);
                $modifyDateTime = strtotime($entry['lastModifiedDateTime']);
                $extGuestId = $entry['extracted_guest_id'] ?? '';
                $extPMSConfNum = $entry['confirmation_number'] ?? null;


                //metaData
                $metaDataArray = [
                    'id' => $entry['id'] ?? null,
                    'import_code' => $entry['import_code'] ?? null,
                    'lastModifiedBy' => $entry['lastModifiedBy'] ?? null,
                    'lastModifiedDateTime' => $entry['lastModifiedDateTime'] ?? null,
                    'lastModifiedDateTime_repo' => $entry['lastModifiedDateTime_repo'] ?? null,
                    'notificationType' => $entry['import_code'] ?? null,
                    'occupancyDetails' => $entry['lastModifiedDateTime_repo'] ?? null,
                    'blocks' => $entry['lastModifiedDateTime_repo'] ?? null,
                    'profiles' => $entry['import_code'] ?? null,
                    'occupiedUnits' => $entry['occupiedUnits'] ?? null

                ];
                $metaDataJson = json_encode($metaDataArray);


                // Create index for stay lookup
                $stayIndex = $extPMSConfNum;
                // Lookup for stayId using the index
                if (isset($indexedReservationStays[$stayIndex])) {
                    $stayId = is_array($indexedReservationStays[$stayIndex])
                        ? reset($indexedReservationStays[$stayIndex])
                        : $indexedReservationStays[$stayIndex];
                } else {
                    $stayId = null;
                }

                // Room type and class lookups

                $className = 'UNKNOWN'; // Replace with actual logic to determine class name
                $libRoomClassId = $indexedRoomClasses[$className] ?? null;

                // Continue building the details array
                $arrRESERVATIONroomDetails[] = [
                    'startDate' => $entry['arrival'] ?? null,
                    'endDate' => $entry['departure'] ?? null,
                    'amount' => $amount,
                    'ratePlanCode' => $ratePlanCode,
                    'isBlocked' => isset($entry['blocks']) && !$entry['blocks']['isempty'] ? 1 : 0,
                    'isComplimentary' => intval($entry['isComplimentary']) ?? null,
                    'isHouseUse' => $entry['isHouseUse'] ?? 0,
                    'metaData' => $metaDataJson,
                    'contactId' => $contactId,
                    'firstName' => $givenName,
                    'lastName' => $surname,
                    'extGuestId' => $extGuestId,
                    'stayId' => $stayId,
                    'createDateTime' => ($timestamp = strtotime($entry['createdDateTime'])) ? strval($timestamp) : null,
                    'modifyDateTime' => ($timestamp = strtotime($entry['lastModifiedDateTime'])) ? strval($timestamp) : null,
                    'extPMSConfNum' => $extPMSConfNum,
                    'dataSource' => 'HAPI',
                    'libRoomId' => $libRoomId,
                    'roomNumber' => $roomNumber,
                    'libRoomTypeId' => $libRoomTypeId,
                    'typeCode' => $typeCode,
                    'libRoomClassId' => $libRoomClassId,
                    'className' => 'UNKNOWN',
                ];
            }
        }

        return $arrRESERVATIONroomDetails;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createArrRESERVATIONroomDetails: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}


function createArrSERVICESfolioOrders($normalizedData, $arrCUSTOMERcontact, $arrRESERVATIONstay, $arrSERVICESpayment, $arrSERVICESlibServiceItems, &$errorCount) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {

        // Indexing customer contacts for fast lookup
        $indexedCustomerContacts = [];
        foreach ($arrCUSTOMERcontact as $contact) {
            $index = $contact['firstName'] . '|' . $contact['lastName'] . '|' . $contact['extGuestId'];
            $indexedCustomerContacts[$index] = $contact['id'];
        }

        // Indexing reservation stays for fast lookup
        $indexedReservationStays = [];
        foreach ($arrRESERVATIONstay as $stay) {
            $index = $stay['extGuestId'] . '|' . $stay['extPMSConfNum'];
            $indexedReservationStays[$index] = $stay['id'];
        }

        // Indexing payments for fast lookup
        $indexedPayments = [];
        foreach ($arrSERVICESpayment as $payment) {
            $index = doubleval($payment['paymentAmount']) . '|' . $payment['currencyCode'];
            $indexedPayments[$index] = $payment['id'];
        }


    // Indexing libServiceItems for fast lookup
        $indexedLibServiceItems = [];
        foreach ($arrSERVICESlibServiceItems as $item) {
            $index = $item['itemCode'] . '|' . $item['ratePlanCode'];
            $indexedLibServiceItems[$index] = $item['id'];
        }

//        // Indexing libFolioOrdersType for fast lookup, adjusted for nested structure
//        $indexedLibFolioOrdersType = [];
//        foreach ($arrSERVICESlibFolioOrdersType as $entry) {
//            $folioOrderTypeData = $entry['SERVICESlibFolioOrdersType'];
//            $indexedLibFolioOrdersType[$folioOrderTypeData['orderType']] = $folioOrderTypeData['id'];
//        }

        $arrSERVICESfolioOrders = [];

        foreach ($normalizedData as $data) {
            foreach ($data['guests'] as $guest) {


                $guestDetails = $guest['guest'] ?? null;
                $firstName = $guestDetails['names'][0]['givenName'] ?? 'UNKNOWN';
                $lastName = $guestDetails['names'][0]['surname'] ?? 'UNKNOWN';
                $extGuestId = $data['extracted_guest_id'] ?? 'UNKNOWN';


//                $isPrimary = isset($guestData['isPrimary']) ? (int)$guestData['isPrimary'] : 0;
                $isPrimary = $guest['isPrimary'] ?? 0;
                $createDateTime = strtotime($data['createdDateTime']) ?? null;
                $modifyDateTime = strtotime($data['lastModifiedDateTime']) ?? null;
                $startDate = $data['arrival'] ?? null; // Assuming these are already in the correct format
                $endDate = $data['departure'] ?? null;
                $createdBy = $data['createdBy'] ?? null;
                $extPMSConfNum = $data['confirmation_number'] ?? '';
                // Populate libServiceItemsId using itemCode and ratePlanCode
                $itemCode = $data['services'][0]['code'] ?? 'UNKNOWN';
                $ratePlanCode = $data['prices'][0]['ratePlanCode'] ?? 'UNKNOWN';


                // Create index for the contact id lookup
                $contactIndex = $firstName . '|' . $lastName . '|' . $extGuestId;
                // Lookup for contact id using the index
                if (isset($indexedCustomerContacts[$contactIndex])) {
                    $contactId = is_array($indexedCustomerContacts[$contactIndex])
                        ? reset($indexedCustomerContacts[$contactIndex])
                        : $indexedCustomerContacts[$contactIndex];
                } else {
                    $contactId = $indexedCustomerContacts['UNKNOWN' . '|' . 'UNKNOWN' . '|' . 'UNKNOWN'];
                }


                // Create index for stay lookup
                $stayIndex = $extGuestId . '|' . $extPMSConfNum;
                // Lookup for stayId using the index
                if (isset($indexedReservationStays[$stayIndex])) {
                    $stayId = is_array($indexedReservationStays[$stayIndex])
                        ? reset($indexedReservationStays[$stayIndex])
                        : $indexedReservationStays[$stayIndex];
                } else {
                    // skip this entry if the record doesn't have a confirmation number or guest Id attached
                    continue;
                }


                $libServiceItemsIndex = $itemCode . '|' . $ratePlanCode;
                $libServiceItemsIdUnKnown = $indexedLibServiceItems[$libServiceItemsIndex] ?? $indexedLibServiceItems['UNKNOWN' . '|' . 'UNKNOWN'];
                // Retrieve the libFolioOrdersTypeId using the folioOrderType


                $segmentations = array_map(function ($segmentation) {
                    return [
                        'Code' => $segmentation['code'] ?? null,
                        'End' => $segmentation['end'] ?? null,
                        'Name' => $segmentation['name'] ?? null, // Assuming 'name' exists
                        'Start' => $segmentation['start'] ?? null,
                        'Type' => $segmentation['type'] ?? null,
                    ];
                }, $data['segmentations'] ?? []);

                $taxes = array_map(function ($tax) {
                    return [
                        'TaxAmount' => $tax['amount'] ?? null, // Assuming 'amount' exists
                        'TaxCode' => $tax['code'] ?? null,
                    ];
                }, $data['taxes'] ?? []);

                $discounts = array_map(function ($discount) {
                    return [
                        'DiscountAmount' => $discount['amount'] ?? null, // Assuming 'amount' exists
                        'DiscountIsIncluded' => $discount['isIncluded'] ?? null,
                        'DiscountStartDateTime' => $discount['start'] ?? null, // Assuming 'start' exists
                        'DiscountEndDateTime' => $discount['end'] ?? null, // Assuming 'end' exists
                    ];
                }, $data['discounts'] ?? []);
                //metaData
                $metaDataArray = [
                    'createdBy' => $data['createdBy'] ?? null,
                    'createdDateTime' => $data['createdDateTime'] ?? null,
                    'createdDateTime_repo' => $data['createdDateTime_repo'] ?? null,
                    'Departure' => $data['departure'] ?? null,
                    'doNotDisplayPrice' => $data['doNotDisplayPrice'] ?? null,
                    'estimatedDateTimeOfArrival' => $data['estimatedDateTimeOfArrival'] ?? null,
                    'estimatedDateTimeOfDeparture' => $data['estimatedDateTimeOfDeparture'] ?? null,
                    'Ext_id' => $data['ext_id'] ?? null,
                    'Ext_status' => $data['ext_status'] ?? null,
                    'guaranteeCode' => $data['guaranteeCode'] ?? null,
                    'OptionDate' => $data['optionDate'] ?? null,
                    'PaymentMethod' => [
                        'Code' => $data['paymentMethod']['code'] ?? null,
                    ],
                    'Prices' => array_map(function ($price) {
                        return [
                            'End' => $price['end'] ?? null,
                            'Start' => $price['start'] ?? null,
                        ];
                    }, $data['prices'] ?? []),
                    'ProcessStamp' => $data['procesststamp'] ?? null,
                    'PromotionCode' => $data['promotionCode'] ?? null,
                    'PurposeOfStay' => $data['purposeOfStay'] ?? null,
                    // Assume ratePlans is similar structure to Prices
                    'ratePlans' => array_map(function ($plan) {
                        return [
                            'Code' => $plan['code'] ?? null,
                            'Description' => $plan['description'] ?? null, // Assuming description exists
                            'End' => $plan['end'] ?? null,
                            'Start' => $plan['start'] ?? null,
                        ];
                    }, $data['ratePlans'] ?? []),
                    'receivedDateTime' => $data['receivedDateTime'] ?? null,
                    // Similar mapping for referenceIDs, AdditionalData, etc.
                    // Due to complexity, these mappings are simplified examples
                    'Command_id' => $data['command_id'] ?? null,
                    'lastModifiedBy' => $data['lastModifiedBy'] ?? null,
                    'isGuestViewable' => $data['isGuestViewable'] ?? null,
                    'lastModifiedDateTime' => $data['lastModifiedDateTime'] ?? null,
                    // Assuming shareIds structure and extracting as needed
                    'shareIds' => array_map(function ($shareId) {
                        return [
                            'Id' => $shareId['id'] ?? null,
                            'IdType' => $shareId['idType'] ?? null,
                            'SystemId' => $shareId['systemId'] ?? null,
                            'SystemType' => $shareId['systemType'] ?? null,
                        ];
                    }, $entry['shareIds'] ?? []),
                    'requestedDeposits' => array_map(function ($deposit) {
                        // Assuming structure and extracting as needed
                        return [
                            'Amount' => $deposit['amount'] ?? null, // Assuming 'amount' exists
                            'Currency' => $deposit['currency'] ?? null, // Assuming 'currency' exists
                        ];
                    }, $data['requestedDeposits'] ?? []),
                    'Alerts' => $data['alerts'] ?? [], // Assuming simple array or further mapping required
                    'contacts' => $data['contacts'] ?? [], // Assuming simple array or further mapping required
                    'Segmentations' => $segmentations,
                    'Taxes' => $taxes,
                    'Discounts' => $discounts
                ];
                $metaDataJson = json_encode($metaDataArray);


//            // Now use the determined folioOrderType for the lookup
//            $libFolioOrdersTypeId = isset($indexedLibFolioOrdersType[$folioOrderType]) ? $indexedLibFolioOrdersType[$folioOrderType] : null;


                // Populate paymentId using paymentAmount and currencyCode
                $paymentAmount = 0;
                $currencyCode = $data['currency']['code'] ?? 'UNKNOWN';
                $paymentIndex = $paymentAmount . '|' . $currencyCode;
                $paymentId = $indexedPayments[$paymentIndex] ?? null;


                // Common fields for all folio orders
                $commonFields = [
                    'dataSource' => 'HAPI',
                    'contactId' => $contactId,
                    'firstName' => $firstName,
                    'lastName' => $lastName,
                    'extGuestId' => $extGuestId,
                    'isPrimary' => $isPrimary,
                    'stayId' => $stayId,
                    'startDateLookup' => $startDate,
                    'endDateLookup' => $endDate,
                    'extPMSConfNum' => $data['confirmation_number'] ?? null,
                    'createDateTime' => $createDateTime ?? null,
                    'modifyDateTime' => $modifyDateTime ?? null,
                    'paymentId' => $paymentId,
                    'paymentAmount' => doubleval($paymentAmount) ?? 0,
                    'currencyCode' => $currencyCode ?? null,
                    'libServiceItemsId' => $libServiceItemsId ?? $libServiceItemsIdUnKnown,
                    'itemCode' => $data['services'][0]['code'] ?? 'UNKNOWN',
                    'ratePlanCode' => $data['prices'][0]['ratePlanCode'] ?? 'UNKNOWN',
                    'metaData' => $metaDataJson

                ];


                // SERVICE type folio order
                if (!empty($data['services'])) {
                    foreach ($data['services'] as $service) {
                        $serviceOrder = $commonFields;
                        $serviceOrder['folioOrderType'] = 'SERVICE';
                        $serviceOrder['unitCount'] = $service['quantity'] ?? null;
                        $serviceOrder['unitPrice'] = null; // Calculate if needed
                        $serviceOrder['fixedCost'] = null; // Calculate if needed
                        $serviceOrder['amountBeforeTax'] = null; // Calculate if needed
                        $serviceOrder['amountAfterTax'] = null; // Calculate if needed
                        $serviceOrder['postingFrequency'] = null;
                        $serviceOrder['startDate'] = $data['bookedUnits'][0]['start'] ?? $startDate;
                        $serviceOrder['endDate'] = $data['bookedUnits'][0]['start'] ?? $endDate;
                        $serviceOrder['amount'] = null;
                        $serviceOrder['fixedChargesQuantity'] = null;
                        $serviceOrder['transferId'] = null;
                        $serviceOrder['transferDateTime'] = null;
                        $serviceOrder['transferOnArrival'] = null;
                        $serviceOrder['isIncluded'] = $service[0]['isIncluded'] ?? 0;
                        // Additional SERVICE specific fields go here...
                        $arrSERVICESfolioOrders[] = $commonFields;
                        $arrSERVICESfolioOrders[] = $serviceOrder;
                    }

                }

                // RESERVATION type folio order
                if ($extPMSConfNum != null) {
                    $reservationOrder = $commonFields;
                    $reservationOrder['folioOrderType'] = 'RESERVATION';
                    $reservationOrder['unitCount'] = null; // Determine if needed
                    $reservationOrder['unitPrice'] = null; // Calculate if needed
                    $reservationOrder['fixedCost'] = $data['prices'][0]['amount'] ?? null;
                    $reservationOrder['amountBeforeTax'] = $data['reservationTotal']['amountBeforeTax'] ?? $data['prices'][0]['amount'];
                    $reservationOrder['amountAfterTax'] = $data['reservationTotal']['amountAfterTax'] ?? $data['prices'][0]['amount'] + $data['taxes'][0]['amount'];
                    $reservationOrder['postingFrequency'] = null;
                    $reservationOrder['startDate'] = $data['bookedUnits'][0]['start'] ?? $startDate;
                    $reservationOrder['endDate'] = $data['bookedUnits'][0]['end'] ?? $endDate;
                    $reservationOrder['amount'] = $data['prices'][0]['amount'] ?? null;
                    $reservationOrder['fixedChargesQuantity'] = null;
                    $reservationOrder['transferId'] = $data['transfer']['id'] ?? null;
                    $reservationOrder['transferDateTime'] = $data['transferDateTime']['dateTime'] ?? null;
                    $reservationOrder['transferOnArrival'] = $data['transferOnArrival']['isOnArrival'] ?? null;
                    $reservationOrder['isIncluded'] = 0;

                    $arrSERVICESfolioOrders[] = $reservationOrder;
                }

                // OTHER type folio order
                if (!empty($data['fixedCharges'])) {
                    foreach ($data['fixedCharges'] as $fixedCharge) {
                        $otherOrder = $commonFields;
                        $otherOrder['folioOrderType'] = 'OTHERS';
                        $otherOrder['unitCount'] = $fixedCharge['quantity'] ?? null;
                        $otherOrder['unitPrice'] = $fixedCharge['amount'] ?? null;
                        $otherOrder['fixedCost'] = ($otherOrder['unitCount'] ?? 0) * ($otherOrder['unitPrice'] ?? 0);
                        // Additional OTHER specific fields go here...
                        $otherOrder['amountBeforeTax'] = $data['prices'][0]['amount'] ?? null;
                        $otherOrder['amountAfterTax'] = $data['prices'][0]['amount'] + $data['taxes'][0]['amount'] ?? null;
                        $otherOrder['postingFrequency'] = $data['fixedCharges'][0]['postingFrequency'] ?? null;
                        $otherOrder['startDate'] = $data['fixedCharges'][0]['start'] ?? $startDate;
                        $otherOrder['endDate'] = $data['fixedCharges'][0]['end'] ?? $endDate;
                        $otherOrder['amount'] = $data['fixedCharges'][0]['amount'] ?? null;
                        $otherOrder['fixedChargesQuantity'] = $data['fixedCharges'][0]['quantity'] ?? null;
                        $otherOrder['transfer'] = null;
                        $otherOrder['transferDateTime'] = null;
                        $otherOrder['transferOnArrival'] = 0;
                        $otherOrder['isIncluded'] = 0;
                        $arrSERVICESfolioOrders[] = $otherOrder;
                    }

                }
//            // Add UNKNOWN type folio order if none of the above apply
//            if (empty($arrSERVICESfolioOrders)) {
//                $unknownOrder = $commonFields;
////            $unknownOrder['folioOrderType'] = 'UNKNOWN';
//                // Add default/unknown values for all other fields
//                $arrSERVICESfolioOrders[] = $unknownOrder;
//            }
            }
        }
//        populateLibFolioOrdersTypeId($arrSERVICESfolioOrders, $arrSERVICESlibFolioOrdersType, $errorCount);

        //Remove any duplicate records
        $arrSERVICESfolioOrders = removeDuplicateRows($arrSERVICESfolioOrders,$errorCount);

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully processed SERVICES folio orders" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);



        return $arrSERVICESfolioOrders;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;
        // Log the main function exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in createArrSERVICESfolioOrders: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}


//function populateLibFolioOrdersTypeId(&$arrSERVICESfolioOrders, $arrSERVICESlibFolioOrdersType, &$errorCount) {
//    // Define the error log file path
//    $errorLogFile = dirname(__FILE__) . '/error_log.txt';
//
//    try {
//        // Indexing libFolioOrdersType for fast lookup
//        $indexedLibFolioOrdersType = [];
//        foreach ($arrSERVICESlibFolioOrdersType as $entry) {
//            $orderType = $entry['orderType'];
//            $orderId = $entry['id'];
//            $indexedLibFolioOrdersType[$orderType] = $orderId;
//        }
//
//        // Loop through each folio order and assign the correct libFolioOrdersTypeId
//        foreach ($arrSERVICESfolioOrders as $key => &$order) {
//            if (isset($order['folioOrderType']) && isset($indexedLibFolioOrdersType[$order['folioOrderType']])) {
//                $order['libFolioOrdersTypeId'] = $indexedLibFolioOrdersType[$order['folioOrderType']];
//            } else {
//                $order['libFolioOrdersTypeId'] = null; // Set to null if no match is found
//            }
//        }
//
//        // Log the success message
//        $errorTimestamp = date('Y-m-d H:i:s');
//        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully populated libFolioOrdersTypeId in folio orders" . PHP_EOL;
//        error_log($successMessage, 3, $errorLogFile);
//    } catch (Exception $e) {
//        // Increment error counter
//        $errorCount++;
//
//        // Log the exception
//        $errorTimestamp = date('Y-m-d H:i:s');
//        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in populateLibFolioOrdersTypeId: " . $e->getMessage() . PHP_EOL;
//        error_log($errorLogMessage, 3, $errorLogFile);
//
//        // Optionally rethrow the exception if further handling is required
//        throw $e;
//    }
//}



function removeDuplicateOrders($arrSERVICESfolioOrders) {
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        // Serialize each order to make it a string
        $serializedOrders = array_map('serialize', $arrSERVICESfolioOrders);

        // Remove duplicates
        $uniqueSerializedOrders = array_unique($serializedOrders);

        // Unserialize each order to convert it back to its original array form
        $uniqueOrders = array_map('unserialize', $uniqueSerializedOrders);

        // Log the success message
        $errorTimestamp = date('Y-m-d H:i:s');
        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully removed duplicate folio orders" . PHP_EOL;
        error_log($successMessage, 3, $errorLogFile);

        return $uniqueOrders;
    } catch (Exception $e) {
        // Increment error counter
        $errorCount++;

        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in removeDuplicateOrders: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}





function normalizeMyDataSemiParsed($myDataSemiParsed)
{
    // Define the error log file path
    $errorLogFile = dirname(__FILE__) . '/error_log.txt';

    try {
        $normalizedData = [];

        foreach ($myDataSemiParsed as $entryKey => $entryValue) {
            // If the value is a JSON string, decode it
            if (is_string($entryValue) && isJson($entryValue)) {
                $decodedJson = json_decode($entryValue, true);
                if (json_last_error() === JSON_ERROR_NONE) {
                    $normalizedData[$entryKey] = $decodedJson;
                } else {
                    // Log a message if there is a JSON error
                    $errorTimestamp = date('Y-m-d H:i:s');
                    $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") JSON decode error in normalizeMyDataSemiParsed for key {$entryKey}: " . json_last_error_msg() . PHP_EOL;
                    error_log($errorLogMessage, 3, $errorLogFile);

                    $normalizedData[$entryKey] = $entryValue;
                }
            } elseif (is_array($entryValue)) {
                // Recursively normalize nested arrays
                $normalizedData[$entryKey] = normalizeMyDataSemiParsed($entryValue);
            } else {
                // Copy over the value directly if it's not a JSON string or an array
                // This also includes copying the title field if it exists.
                $normalizedData[$entryKey] = $entryValue;
            }
        }
        // Log the success message
//        $errorTimestamp = date('Y-m-d H:i:s');
//        $successMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Successfully normalized semi-parsed data" . PHP_EOL;
//        error_log($successMessage, 3, $errorLogFile);

        return $normalizedData;
    } catch (Exception $e) {

        // Log the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] (".__FUNCTION__.") Error in normalizeMyDataSemiParsed: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);

        // Optionally rethrow the exception if further handling is required
        throw $e;
    }
}

function cleanLogs($daysToKeep, $logFilePath) {
    // Check if log file exists
    if (!file_exists($logFilePath)) {
        throw new Exception("Log file does not exist: $logFilePath");
    }

    // Read the contents of the log file
    $logEntries = file($logFilePath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    if (!$logEntries) {
        // Log file is empty or unreadable
        return;
    }

    // Determine the cutoff date
    $cutoffDate = new DateTime();
    $cutoffDate->modify("-$daysToKeep days");

    // Filter out old log entries
    $filteredEntries = array_filter($logEntries, function($entry) use ($cutoffDate) {
        // Extract the timestamp from the log entry
        preg_match("/^\[(.*?)\]/", $entry, $matches);
        $entryDate = DateTime::createFromFormat('Y-m-d H:i:s', $matches[1] ?? '');

        // Keep the entry if its date is after the cutoff date
        return $entryDate >= $cutoffDate;
    });

    // Rewrite the log file with the filtered entries
    file_put_contents($logFilePath, implode(PHP_EOL, $filteredEntries) . PHP_EOL);
}


function isJson($string) {
    json_decode($string);
    return json_last_error() === JSON_ERROR_NONE;
}
?>