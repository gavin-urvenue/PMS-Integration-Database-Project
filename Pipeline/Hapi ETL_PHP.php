<?php

//variables
$schemaVersion = 1.38;
// Database Connections:
$originTableName = 'hapi_raw_reservations';
$originHost = 'localhost:3306';
$originUsername = 'urvenue';
$originPassword = 'Password1!';
$originDatabase = 'Testing';

$destinationHost = 'localhost:3306';
$destinationUsername = 'urvenue';
$destinationPassword = 'Password1!';
$destinationDBName = 'pms_db';

// Create connections
$destinationDBConnection = new mysqli($destinationHost, $destinationUsername, $destinationPassword, $destinationDBName);

$originDBConnection = new mysqli($originHost, $originUsername, $originPassword, $originDatabase);


//Functions and Libraries
////Function to remove duplicate records from an array based on 2 fields
function removeDuplicateRows2D($data, $key1, $key2)
{
    $uniqueKeys = [];

    return array_filter($data, function ($item) use ($key1, $key2, &$uniqueKeys) {
        $value1 = $item[$key1];
        $value2 = $item[$key2];

        $hash = md5($value1 . $value2);

        if (!isset($uniqueKeys[$hash])) {
            $uniqueKeys[$hash] = true;
            return true;
        }

        return false;
    });
}
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

function fetchDataFromMySQLTable($tableName, $originDBConnection, $destinationDBConnection, &$insertCount, &$updateCount) {


    try {
        // Check connection
        if ($originDBConnection->connect_error) {
            throw new Exception('Connection failed: ' . $originDBConnection->connect_error);
        }

        // Start transaction
        $originDBConnection->begin_transaction();

        // Fetch the last etlStartTStamp from PMSDATABASEmisc
        $lastEtlStartTStamp = getLatestEtlTimestamp($destinationDBConnection);

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
        $successMessage = "Successfully fetched $recordCount records from $tableName";
        error_log($successMessage, 3, 'error_log.txt');

        return $data;

    } catch (Exception $e) {
        // Rollback the transaction on error
        $originDBConnection->rollback();

        // Log the error
        error_log($e->getMessage(), 3, 'error_log.txt');

        // Optionally rethrow the exception if you want to handle it further up the call stack
        throw $e;
    }
}


function insertEtlTrackingInfo($destinationDBConnection, $insertCount, $updateCount, $etlSource, &$schemaVersion) {
    try {
        // Check connection
        if ($destinationDBConnection->connect_error) {
            throw new Exception('Connection failed: ' . $destinationDBConnection->connect_error);
        }

        // Prepare the file path and etlSource for SQL insertion
        // Define the error log file path
        $etlLogFile = dirname(__FILE__) . '/error_log.txt'; // Path to error_log.txt in the same directory as this script

        $etlSource = $destinationDBConnection->real_escape_string($etlSource); // Escaping the etlSource

        // Prepare and execute insert query with additional fields for insertCount and updateCount
        $etlStartTStamp = time(); // Current Unix timestamp
        $query = "INSERT INTO PMSDATABASEmisc (schemaVersion, etlStartTStamp, etlInsertsCount, etlUpdatesCount, etlLogFile, etlSource) VALUES ($schemaVersion, $etlStartTStamp, $insertCount, $updateCount, '$etlLogFile', '$etlSource')";
        $result = $destinationDBConnection->query($query);

        // Check for query success
        if (!$result) {
            throw new Exception('Query failed: ' . $destinationDBConnection->error);
        }

        // Log the success message
        $successMessage = "Successfully inserted tracking info into PMSDATABASEmisc";
        error_log($successMessage, 3, 'error_log.txt');

    } catch (Exception $e) {
        // Log the error
        error_log($e->getMessage(), 3, 'error_log.txt');

        // Optionally rethrow the exception if you want to handle it further up the call stack
        throw $e;
    }
}





function updateEtlDuration($destDBConnection)
{
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
        $etlStartTStamp = $resultStartTStamp->fetch_assoc()['etlStartTStamp'] ?? 0;

        // Calculate the duration
        $etlDuration = $currentTimestamp - $etlStartTStamp;

        // Update the latest ETL record with the end timestamp and duration
        $updateQuery = "UPDATE PMSDATABASEmisc SET etlEndTStamp = $currentTimestamp, etlDuration = $etlDuration ORDER BY id DESC LIMIT 1";
        $destDBConnection->query($updateQuery);

    } catch (Exception $e) {
        // Handle exceptions, such as logging and re-throwing
        error_log($e->getMessage(), 3, 'error_log.txt');
        throw $e;
    }
}

function getFirstNonNullImportCode($originDBConnection, $tableName)
{
    $importCode = '';

    try {
        $importCodeQuery = "SELECT import_code FROM $tableName WHERE import_code IS NOT NULL LIMIT 1";
        $importCodeResult = $originDBConnection->query($importCodeQuery);

        if (!$importCodeResult) {
            throw new Exception('Query failed: ' . $originDBConnection->error);
        }

        if ($row = $importCodeResult->fetch_assoc()) {
            $importCode = $row['import_code'];
        }

    } catch (Exception $e) {
        error_log("Error fetching import_code: " . $e->getMessage(), 3, 'error_log.txt');
    }

    return $importCode;
}



////Function to create and populate the ReservationlibProperty Table Associative Array
function createReservationLibProperty($reservations)
{
    $reservationLibProperty = [];
    $seenCodes = [];



    foreach ($reservations as $reservation) {
        $propertyCode = $reservation['extracted_property_code'];
        $chainCode = $reservation['extracted_chain_code'];

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

    // Add a row with unknown propertyCode
    $unknownRow = [
        'propertyCode' => 'UNKNOWN',
        'chainCode' => 'UNKNOWN',
        'dataSource' => 'HAPI',
    ];
    array_unshift($reservationLibProperty, $unknownRow);

    return $reservationLibProperty;
}

////Function to create and populate the ReservationlibSource Table Associative Array
function createReservationLibSource($data)
{
    if (empty($data)) {
        echo "Invalid data array\n";
        return;
    }

    $result = [];

    // Add initial row with Unknown values
    $result[] = [
        'RESERVATIONlibsource' => [
            'sourceName' => 'UNKNOWN',
            'sourceType' => 'UNKNOWN',
            'dataSource' => 'HAPI',
        ],
    ];

    foreach ($data as $profile) {
        if (!empty($profile['profiles'])) {
            $profiles = json_decode($profile['profiles'], true);

            foreach ($profiles as $profileData) {
                $reservationLibSource = [
                    'RESERVATIONlibsource' => [
                        'sourceName' => $profileData['names'][0]['name'] ?? "",
                        'sourceType' => $profileData['type'] ?? "",
                        'dataSource' => 'HAPI',
                    ],
                    // You can add more fields here based on your mapping
                    // Example:
                    // 'additionalField' => $profileData['someValue'] ?? "",
                ];

                // Check for duplicate rows
                if (!in_array($reservationLibSource, $result)) {
                    $result[] = $reservationLibSource;
                }
            }
        }
    }

    // Add a row with unknown propertyCode
    $unknownRow = [
        'sourceName' => 'UNKNOWN',
        'sourceType' => 'UNKNOWN',
        'dataSource' => 'HAPI',
    ];
    array_unshift($reservationLibSource, $unknownRow);

    return $result;
}

////Function to create and populate the ReservationlibRoomType Table Associative Array
function createReservationLibRoomType($data)
{
    if (empty($data)) {
        echo "Invalid data array\n";
        return;
    }

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
                        'typeName' => '',
                        'typeCode' => $unitTypeCode,
                        'dataSource' => 'HAPI',
                    ],
                    // You can add more fields here based on your mapping
                    // Example:
                    // 'additionalField' => $unit['someValue'] ?? "",
                ];

                // Check for duplicate rows
                if (!in_array($reservationLibRoomType, $result)) {
                    $result[] = $reservationLibRoomType;
                }
            }
        }
    }

    return $result;
}

////Function to create and populate the ReservationLibStayStatus Table Associative Array
function createReservationLibStayStatus($data)
{
    if (empty($data)) {
        echo "Invalid data array\n";
        return;
    }

    $result = [];

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
            // You can add more fields here based on your mapping
            // Example:
            // 'additionalField' => $reservation['someValue'] ?? "",
        ];

        // Check for duplicate rows
        if (!in_array($reservationLibStayStatus, $result)) {
            $result[] = $reservationLibStayStatus;
        }
    }

    return $result;
}

////Function to create and populate the ReservationGroup Table Associative Array
function createReservationGroup($data)
{
    if (empty($data)) {
        echo "Invalid data array\n";
        return;
    }

    $result = [];

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
            // You can add more fields here based on your mapping
            // Example:
            // 'additionalField' => $reservation['someValue'] ?? "",
        ];

        // Check for duplicate rows
        if (!in_array($reservationGroup, $result)) {
            $result[] = $reservationGroup;
        }
    }

    return $result;
}

////Function to create and populate the ReservationLibRoomClass Table Associative Array
function createReservationLibRoomClass($data)
{
    if (empty($data)) {
        echo "Invalid data array\n";
        return;
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


    return $result;
}

////Function to create and populate the ReservationLibRoom Table Associative Array
function createReservationLibRoom($array) {
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
                // Log an error or handle it accordingly if JSON is not valid
                error_log("JSON decode error in occupiedUnits: " . json_last_error_msg());
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

    return $reservationRooms;
}


////Function to create and populate the CUSTOMERLibLoyaltyProgram Table Associative Array
function createCUSTOMERloyaltyProgram() {
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

    return $loyaltyProgramArray;
}

function createCUSTOMERContactType() {
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

    return $contactTypeArray;
}



////Function to create and populate the SERVICESLibTender Table Associative Array
function createSERVICESLibTender($data)
{
    $result[] = [
        'paymentMethod' => 'UNKNOWN',
        'dataSource' => 'HAPI',
    ];

    foreach ($data as $record) {
        $paymentMethod = $record['paymentMethod'] ?? null;

        if ($paymentMethod) {
            $code = substr($paymentMethod, 10, 2); // Extract the first 2 letters
            $result[] = [
                'paymentMethod' => $code,
                'dataSource' => 'HAPI',
            ];
        }
    }

    return removeDuplicateRows2D($result, 'paymentMethod', 'dataSource');
}

////Function to create and populate the SERVICESlibServiceItems Table Associative Array
function createSERVICESlibserviceitems($array) {
    $serviceItems = [
        [ // Add the 'UNKNOWN' record at the beginning of the array
            'itemName' => 'UNKNOWN',
            'itemCode' => 'UNKNOWN',
            'ratePlanCode' => 'UNKNOWN',
            'dataSource' => 'HAPI'
        ]
    ];

    $uniqueCheck = []; // Array to keep track of existing items to prevent duplicates

    foreach ($array as $item) {
        $ratePlans = isset($item['ratePlans']) ? json_decode($item['ratePlans'], true) : [];
        $prices = isset($item['prices']) ? json_decode($item['prices'], true) : [];

        // Initialize service item data with default values
        $serviceItemData = [
            'itemName' => '', // As per mapping, itemName remains empty
            'itemCode' => '',
            'ratePlanCode' => '',
            'dataSource' => 'HAPI' // Data source is always 'HAPI'
        ];

        // Extract item code from Services
        if (!empty($ratePlans) && isset($ratePlans[0]['code'])) {
            $serviceItemData['itemCode'] = $ratePlans[0]['code'];
        }

        // Extract rate plan code from Prices
        if (!empty($prices) && isset($prices[0]['ratePlanCode'])) {
            $serviceItemData['ratePlanCode'] = $prices[0]['ratePlanCode'];
        }

        // Create a unique key to check for duplicates
        $uniqueKey = $serviceItemData['itemCode'] . '|' . $serviceItemData['ratePlanCode'];
        if (!empty($serviceItemData['itemCode']) && !empty($serviceItemData['ratePlanCode']) && !isset($uniqueCheck[$uniqueKey])) {
            $serviceItems[] = $serviceItemData;
            $uniqueCheck[$uniqueKey] = true; // Mark this key as seen
        }
    }

    return $serviceItems;
}


////Function to create and populate the SERVICESlibFolioOrderType Table Associative Array
function createSERVICESlibFolioOrderType() {
    $FolioOrderTypeArray = [
        [
            'SERVICESlibFolioOrdersType' => [
                'orderType' => 'UNKNOWN',
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'SERVICESlibFolioOrdersType' => [
                'orderType' => 'RESERVATION',
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'SERVICESlibFolioOrdersType' => [
                'orderType' => 'SERVICE',
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'SERVICESlibFolioOrdersType' => [
                'orderType' => 'RESORT FEE',
                'dataSource' => 'HAPI'
            ]
        ],
    ];

    return $FolioOrderTypeArray;
}

////Function to create and populate the CUSTOMERcontact Table Associative Array
function createCUSTOMERcontact($array) {
    $customerContacts = [
        [ // Add a 'UNKNOWN' record at the beginning of the array
            'firstName' => 'UNKNOWN',
            'lastName' => 'UNKNOWN',
            'title' => 'UNKNOWN',
            'email' => 'UNKNOWN',
            // 'birthDate' => 'UNKNOWN',
            'languageCode' => 'UNKNOWN',
            'languageFormat' => 'UNKNOWN',
            'extGuestId' => 'UNKNOWN',
            // 'isPrimary' => 'UNKNOWN',
            'dataSource' => 'HAPI'
        ]
    ];

    $uniqueCheck = []; // Array to keep track of existing contacts to prevent duplicates

    foreach ($array as $item) {
        if (isset($item['guests']) && $item['guests'] !== null) {
            $guests = json_decode($item['guests'], true);

            if (json_last_error() !== JSON_ERROR_NONE) {
                // Handle the JSON error accordingly
                continue;
            }

            foreach ($guests as $guest) {
                if (!isset($guest['guest'])) {
                    continue;
                }
                $guestData = $guest['guest'];

                // Initialize the contact array with default values
                $contact = [
                    'firstName' => $guestData['names'][0]['givenName'] ?? '',
                    'lastName' => $guestData['names'][0]['surname'] ?? '',
                    'title' => $guestData['names'][0]['title'] ?? '',
                    'email' => '',
                    'birthDate' => $guestData['dateOfBirth'] ?? null, // Assign birthDate from dateOfBirth
                    'languageCode' => $guestData['primaryLanguage']['code'] ?? '',
                    'languageFormat' => $guestData['primaryLanguage']['format'] ?? '',
                    'extGuestId' => $item['extracted_guest_id'] ?? '',
                    'isPrimary' => $guest['isPrimary'] ?? '',
                    'dataSource' => 'HAPI'
                ];

                // Extract email if contact details are provided
                if (isset($guestData['contactDetails'])) {
                    foreach ($guestData['contactDetails'] as $detail) {
                        if ($detail['type'] === 'EMAIL' && isset($detail['value'])) {
                            $contact['email'] = $detail['value'];
                            break; // Assuming we need only the first email
                        }
                    }
                }

                // Use a combination of name and email to check for uniqueness
                $uniqueId = $contact['firstName'] . '|' . $contact['lastName'] . '|' . $contact['email'];
                if (!isset($uniqueCheck[$uniqueId])) {
                    $customerContacts[] = $contact;
                    $uniqueCheck[$uniqueId] = true;
                }
            }
        }
    }

    return $customerContacts;
}

//function to insert associative array data into a MySQL table with the same fields
function ParentTableValidation($tableName, $arrayOfAssocArrays, $dbConnection) {
// Define the schema for each table
    $tableSchemas = [
        'CUSTOMERcontact' => [
            'firstName' => ['type' => 'varchar', 'length' => 32],
            'lastName' => ['type' => 'varchar', 'length' => 32],
            'title' => ['type' => 'varchar', 'length' => 32],
            'email' => ['type' => 'varchar', 'length' => 64],
            'birthDate' => ['type' => 'date'],
            'languageCode' => ['type' => 'varchar', 'length' => 16],
            'languageFormat' => ['type' => 'varchar', 'length' => 32],
            'extGuestId' => ['type' => 'varchar', 'length' => 45],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'RESERVATIONlibRoom' => [
            'roomNumber' => ['type' => 'varchar', 'length' => 32],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'RESERVATIONlibSource' => [
            'sourceName' => ['type' => 'varchar', 'length' => 64],
            'sourceType' => ['type' => 'varchar', 'length' => 32],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'RESERVATIONlibProperty' => [
            'chainCode' => ['type' => 'varchar', 'length' => 32],
            'propertyCode' => ['type' => 'varchar', 'length' => 32],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'CUSTOMERlibLoyaltyProgram' => [
            'name' => ['type' => 'varchar', 'length' => 64],
            'source' => ['type' => 'varchar', 'length' => 64],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'SERVICESlibTender' => [
            'paymentMethod' => ['type' => 'varchar', 'length' => 64],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'SERVICESlibServiceItems' => [
            'itemName' => ['type' => 'varchar', 'length' => 64],
            'itemCode' => ['type' => 'varchar', 'length' => 64],
            'ratePlanCode' => ['type' => 'varchar', 'length' => 64],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'SERVICESlibFolioOrdersType' => [
            'orderType' => ['type' => 'varchar', 'length' => 32],
            'metaData' => ['type' => 'json'],
            // 'dataSource' field is not mentioned in this table's structure
        ],
        // Add more tables as per your database schema
        'RESERVATIONgroup' => [
            'groupName' => ['type' => 'varchar', 'length' => 64],
            'groupNumber' => ['type' => 'varchar', 'length' => 64],
            'groupStartDate' => ['type' => 'date'],
            'groupEndDate' => ['type' => 'date'],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'CUSTOMERlibContactType' => [
            'type' => ['type' => 'varchar', 'length' => 32],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'RESERVATIONlibStayStatus' => [
            'statusName' => ['type' => 'varchar', 'length' => 64],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'RESERVATIONlibRoomType' => [
            'typeName' => ['type' => 'varchar', 'length' => 64],
            'typeCode' => ['type' => 'varchar', 'length' => 32],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
        'RESERVATIONlibRoomClass' => [
            'className' => ['type' => 'varchar', 'length' => 32],
            'metaData' => ['type' => 'json'],
            'dataSource' => ['type' => 'varchar', 'length' => 24],
        ],
    ];

// Check if the table name is valid
    if (!array_key_exists($tableName, $tableSchemas)) {
        throw new Exception("Invalid table name: $tableName");
    }

// Iterate over each associative array in the array of associative arrays
foreach ($arrayOfAssocArrays as $assocArray) {
    // Validate the associative array against the table schema
    foreach ($assocArray as $key => $value) {
        if (!array_key_exists($key, $tableSchemas[$tableName])) {
            throw new Exception("Invalid field: $key for table: $tableName");
        }


        // Type and length check
        $fieldInfo = $tableSchemas[$tableName][$key];
        $expectedType = $fieldInfo['type'];
        $expectedLength = $fieldInfo['length'] ?? PHP_INT_MAX; // Default to a large number if length is not set

        // Check type
        if ($expectedType == 'varchar') {
            if (!is_string($value)) {
                throw new Exception("Type mismatch: Expected string for field $key, got " . gettype($value));
            }
        } elseif ($expectedType == 'int') {
            if (!is_int($value)) {
                throw new Exception("Type mismatch: Expected integer for field $key, got " . gettype($value));
            }
        } elseif ($expectedType == 'date') {
            if (!is_string($value) || !preg_match('/^\d{4}-\d{2}-\d{2}$/', $value)) { // Simple regex for date format check
                throw new Exception("Type mismatch or format error: Expected date (YYYY-MM-DD) for field $key");
            }
        } elseif ($expectedType == 'json') {
            if (!is_string($value) || is_null(json_decode($value))) {
                throw new Exception("Type mismatch or format error: Expected JSON string for field $key");
            }
        }
        // Add more type checks as per your requirements

        // Check length
        if (is_string($value) && strlen($value) > $expectedLength) {
            throw new Exception("Length error: Data too long for field $key. Expected maximum length $expectedLength");
        }
    }


}
}
function upsertCustomerContactType($data, $dbConnection) {
    $tableName = 'CUSTOMERlibContactType';

    foreach ($data as $element) {
        if (isset($element[$tableName])) {
            $type = $element[$tableName]['type'];
            $dataSource = $element[$tableName]['dataSource'];

            // Start a transaction
            $dbConnection->begin_transaction();

            try {
                // Check if a record with this type already exists
                $checkQuery = "SELECT `id` FROM `$tableName` WHERE `type` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                $stmt->bind_param("s", $type);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                // Upsert query
                if ($exists) {
                    // Update
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ? WHERE `type` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    $updateStmt->bind_param("ss", $dataSource, $type);
                    $updateStmt->execute();
                } else {
                    // Insert
                    $insertQuery = "INSERT INTO `$tableName` (`type`, `dataSource`) VALUES (?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    $insertStmt->bind_param("ss", $type, $dataSource);
                    $insertStmt->execute();
                }

                // Commit the transaction
                $dbConnection->commit();

            } catch (Exception $e) {
                // Rollback the transaction on error
                $dbConnection->rollback();
                throw $e;  // Re-throw the exception
            }
        } else {
            throw new Exception("Invalid data structure.");
        }
    }
}

function upsertCustomerContact($data, $dbConnection) {
    $tableName = 'CUSTOMERcontact';

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
            throw $e;  // Re-throw the exception
        }
    }
}

function upsertReservationLibRoom($data, $dbConnection) {
    $tableName = 'RESERVATIONlibRoom';

    foreach ($data as $record) {
        // Extract the required fields
        $roomNumber = $record['roomNumber'] ?? null;
        $dataSource = $record['dataSource'] ?? null;
        $metaData = $record['metaData'] ?? null;  // Assuming metaData is optional

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
            // Check if a record with this room number already exists
            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `roomNumber` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("s", $roomNumber);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            // Upsert query
            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `metaData` = ?, `dataSource` = ? WHERE `roomNumber` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("sss", $metaData, $dataSource, $roomNumber);
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`roomNumber`, `metaData`, `dataSource`) VALUES (?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("sss", $roomNumber, $metaData, $dataSource);
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
            throw $e;  // Re-throw the exception
        }
    }
}

function upsertReservationLibSource($data, $dbConnection) {
    $tableName = 'RESERVATIONlibSource';

    foreach ($data as $element) {
        if (isset($element['RESERVATIONlibsource'])) {  // Ensure this key matches your actual data structure
            $record = $element['RESERVATIONlibsource'];  // Same here

            // Extract the required fields
            $sourceName = $record['sourceName'] ?? null;
            $sourceType = $record['sourceType'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = $record['metaData'] ?? null;  // Assuming metaData is optional

            // Start a transaction
            $dbConnection->begin_transaction();

            try {
                // Check if a record with this combination already exists
                $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `sourceName` = ? AND `sourceType` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                $stmt->bind_param("ss", $sourceName, $sourceType);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_row()[0] > 0;

                // Upsert query
                if ($exists) {
                    // Update
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `sourceName` = ? AND `sourceType` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    $updateStmt->bind_param("ssss", $dataSource, $metaData, $sourceName, $sourceType);
                } else {
                    // Insert
                    $insertQuery = "INSERT INTO `$tableName` (`sourceName`, `sourceType`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    $insertStmt->bind_param("ssss", $sourceName, $sourceType, $dataSource, $metaData);
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
                throw $e;  // Re-throw the exception
            }
        } else {
            throw new Exception("Invalid data structure.");
        }
    }
}

function upsertReservationLibProperty($data, $dbConnection) {
    $tableName = 'RESERVATIONlibProperty';

    foreach ($data as $record) {
        // Extract the required fields
        $propertyCode = $record['propertyCode'] ?? null;
        $chainCode = $record['chainCode'] ?? null;
        $dataSource = $record['dataSource'] ?? null;
        $metaData = $record['metaData'] ?? null;  // Assuming metaData is optional

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `propertyCode` = ? AND `chainCode` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("ss", $propertyCode, $chainCode);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            // Upsert query
            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `propertyCode` = ? AND `chainCode` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("ssss", $dataSource, $metaData, $propertyCode, $chainCode);
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`propertyCode`, `chainCode`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("ssss", $propertyCode, $chainCode, $dataSource, $metaData);
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
            throw $e;  // Re-throw the exception
        }
    }
}

function upsertCustomerLibLoyaltyProgram($data, $dbConnection) {
    $tableName = 'CUSTOMERlibLoyaltyProgram';

    foreach ($data as $element) {
        if (isset($element['CUSTOMERlibLoyaltyProgram'])) {
            $record = $element['CUSTOMERlibLoyaltyProgram'];
            // Extract the required fields
            $name = $record['Name'] ?? null;
            $source = $record['Source'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []);  // Convert metaData array to JSON string

            // Start a transaction
            $dbConnection->begin_transaction();

            try {
                // Check if a record with this combination already exists
                $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `name` = ? AND `source` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                $stmt->bind_param("ss", $name, $source);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_row()[0] > 0;

                // Upsert query
                if ($exists) {
                    // Update
                    $updateQuery = "UPDATE `$tableName` SET `metaData` = ?, `dataSource` = ? WHERE `name` = ? AND `source` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    $updateStmt->bind_param("ssss", $metaData, $dataSource, $name, $source);
                } else {
                    // Insert
                    $insertQuery = "INSERT INTO `$tableName` (`name`, `source`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    $insertStmt->bind_param("ssss", $name, $source, $dataSource, $metaData);
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
                throw $e;  // Re-throw the exception
            }
        } else {
            throw new Exception("Invalid data structure.");
        }
    }
}

function upsertServicesLibTender($data, $dbConnection) {
    $tableName = 'SERVICESlibTender';

    foreach ($data as $record) {
        // Extract the required fields
        $paymentMethod = $record['paymentMethod'] ?? null;
        $dataSource = $record['dataSource'] ?? null;
        $metaData = json_encode($record['metaData'] ?? []);  // Convert metaData array to JSON string

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
            // Check if a record with this payment method already exists
            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `paymentMethod` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("s", $paymentMethod);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            // Upsert query
            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `paymentMethod` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("sss", $dataSource, $metaData, $paymentMethod);
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`paymentMethod`, `dataSource`, `metaData`) VALUES (?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("sss", $paymentMethod, $dataSource, $metaData);
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
            throw $e;  // Re-throw the exception
        }
    }
}

function upsertServicesLibServiceItems($data, $dbConnection) {
    $tableName = 'SERVICESlibServiceItems';

    foreach ($data as $record) {
        // Extract the required fields
        $itemName = $record['itemName'] ?? null;
        $itemCode = $record['itemCode'] ?? null;
        $ratePlanCode = $record['ratePlanCode'] ?? null;
        $dataSource = $record['dataSource'] ?? null;
        $metaData = json_encode($record['metaData'] ?? []);  // Convert metaData array to JSON string

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `itemName` = ? AND `itemCode` = ? AND `ratePlanCode` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("sss", $itemName, $itemCode, $ratePlanCode);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            // Upsert query
            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `itemName` = ? AND `itemCode` = ? AND `ratePlanCode` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("sssss", $dataSource, $metaData, $itemName, $itemCode, $ratePlanCode);
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`itemName`, `itemCode`, `ratePlanCode`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("sssss", $itemName, $itemCode, $ratePlanCode, $dataSource, $metaData);
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
            throw $e;  // Re-throw the exception
        }
    }
}

function upsertServicesLibFolioOrdersType($data, $dbConnection) {
    $tableName = 'SERVICESlibFolioOrdersType';

    foreach ($data as $element) {
        if (isset($element[$tableName])) {
            $record = $element[$tableName];

            // Extract the required fields
            $orderType = $record['orderType'] ?? null;
            if ($orderType === null || $orderType === '') {
                error_log("Skipped a record due to missing orderType", 3, 'error_log.txt');
                continue; // Skip this record if orderType is not set or is an empty string
            }

            $metaData = json_encode($record['metaData'] ?? []);  // Convert metaData array to JSON string

            // Start a transaction
            $dbConnection->begin_transaction();

            try {
                // Check if a record with this order type already exists
                $checkQuery = "SELECT `id` FROM `$tableName` WHERE `orderType` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                $stmt->bind_param("s", $orderType);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                // Upsert query
                if ($exists) {
                    // Update
                    $updateQuery = "UPDATE `$tableName` SET `metaData` = ? WHERE `orderType` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    $updateStmt->bind_param("ss", $metaData, $orderType);
                } else {
                    // Insert
                    $insertQuery = "INSERT INTO `$tableName` (`orderType`, `metaData`) VALUES (?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    $insertStmt->bind_param("ss", $orderType, $metaData);
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
                throw $e;
            }
        } else {
            throw new Exception("Invalid data structure.");
        }
    }
}


function upsertReservationGroup($data, $dbConnection) {
    $tableName = 'RESERVATIONgroup';

    foreach ($data as $element) {
        if (isset($element['RESERVATIONgroup'])) {
            $record = $element['RESERVATIONgroup'];

            // Extract the required fields
            $groupName = $record['groupName'] ?? null;
            $groupNumber = $record['groupNumber'] ?? null;
            $groupStartDate = $record['groupStartDate'] ?? null;
            $groupEndDate = $record['groupEndDate'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []);

            // Handle NULL dates properly
            $groupStartDate = !empty($groupStartDate) ? $groupStartDate : null;
            $groupEndDate = !empty($groupEndDate) ? $groupEndDate : null;

            // Start a transaction
            $dbConnection->begin_transaction();

            try {
                // Prepare the check query
                $stmt = $dbConnection->prepare("SELECT `id` FROM `$tableName` WHERE `groupName` = ? AND `groupNumber` = ? AND (`groupStartDate` = ? OR `groupStartDate` IS NULL) AND (`groupEndDate` = ? OR `groupEndDate` IS NULL)");
                $stmt->bind_param("ssss", $groupName, $groupNumber, $groupStartDate, $groupEndDate);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                // Upsert query
                if ($exists) {
                    // Update
                    $updateStmt = $dbConnection->prepare("UPDATE `$tableName` SET `metaData` = ?, `dataSource` = ? WHERE `groupName` = ? AND `groupNumber` = ? AND (`groupStartDate` = ? OR `groupStartDate` IS NULL) AND (`groupEndDate` = ? OR `groupEndDate` IS NULL)");
                    $updateStmt->bind_param("ssssss", $metaData, $dataSource, $groupName, $groupNumber, $groupStartDate, $groupEndDate);
                } else {
                    // Insert
                    $insertStmt = $dbConnection->prepare("INSERT INTO `$tableName` (`groupName`, `groupNumber`, `groupStartDate`, `groupEndDate`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?, ?, ?)");
                    $insertStmt->bind_param("ssssss", $groupName, $groupNumber, $groupStartDate, $groupEndDate, $dataSource, $metaData);
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
                throw $e;
            }
        } else {
            throw new Exception("Invalid data structure.");
        }
    }
}


function upsertReservationLibStayStatus($data, $dbConnection) {
    $tableName = 'RESERVATIONlibStayStatus';

    foreach ($data as $element) {
        if (isset($element['RESERVATIONlibstaystatus'])) {
            $record = $element['RESERVATIONlibstaystatus'];

            // Extract the required fields
            $statusName = $record['statusName'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []);  // Convert metaData array to JSON string

            // Start a transaction
            $dbConnection->begin_transaction();

            try {
                // Check if a record with this status name already exists
                $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `statusName` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                $stmt->bind_param("s", $statusName);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_row()[0] > 0;

                // Upsert query
                if ($exists) {
                    // Update
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `statusName` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    $updateStmt->bind_param("sss", $dataSource, $metaData, $statusName);
                } else {
                    // Insert
                    $insertQuery = "INSERT INTO `$tableName` (`statusName`, `dataSource`, `metaData`) VALUES (?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    $insertStmt->bind_param("sss", $statusName, $dataSource, $metaData);
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
                throw $e;
            }
        } else {
            throw new Exception("Invalid data structure.");
        }
    }
}

function upsertReservationLibRoomType($data, $dbConnection) {
    $tableName = 'RESERVATIONlibRoomType';

    foreach ($data as $element) {
        if (isset($element['ReservationLibRoomType'])) { // Adjusted to match your array key
            $record = $element['ReservationLibRoomType']; // Adjusted to match your array key

            // Extract the required fields
            $typeName = $record['typeName'] ?? null;
            $typeCode = $record['typeCode'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []);  // Convert metaData array to JSON string

            // Start a transaction
            $dbConnection->begin_transaction();

            try {
                // Check if a record with this combination already exists
                $checkQuery = "SELECT `id` FROM `$tableName` WHERE `typeName` = ? AND `typeCode` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                $stmt->bind_param("ss", $typeName, $typeCode);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                // Upsert query
                if ($exists) {
                    // Update
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `typeName` = ? AND `typeCode` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    $updateStmt->bind_param("ssss", $dataSource, $metaData, $typeName, $typeCode);
                } else {
                    // Insert
                    $insertQuery = "INSERT INTO `$tableName` (`typeName`, `typeCode`, `dataSource`, `metaData`) VALUES (?, ?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    $insertStmt->bind_param("ssss", $typeName, $typeCode, $dataSource, $metaData);
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
                throw $e;
            }
        } else {
            throw new Exception("Invalid data structure.");
        }
    }
}

function upsertReservationLibRoomClass($data, $dbConnection) {
    $tableName = 'RESERVATIONlibRoomClass';

    foreach ($data as $element) {
        if (isset($element['RESERVATIONlibRoomClass'])) {
            $record = $element['RESERVATIONlibRoomClass'];

            // Extract the required fields
            $className = $record['className'] ?? null;
            $dataSource = $record['dataSource'] ?? null;
            $metaData = json_encode($record['metaData'] ?? []);  // Convert metaData array to JSON string

            // Start a transaction
            $dbConnection->begin_transaction();

            try {
                // Check if a record with this class name already exists
                $checkQuery = "SELECT `id` FROM `$tableName` WHERE `className` = ?";
                $stmt = $dbConnection->prepare($checkQuery);
                $stmt->bind_param("s", $className);
                $stmt->execute();
                $result = $stmt->get_result();
                $exists = $result->fetch_assoc();

                // Upsert query
                if ($exists) {
                    // Update
                    $updateQuery = "UPDATE `$tableName` SET `dataSource` = ?, `metaData` = ? WHERE `className` = ?";
                    $updateStmt = $dbConnection->prepare($updateQuery);
                    $updateStmt->bind_param("sss", $dataSource, $metaData, $className);
                } else {
                    // Insert
                    $insertQuery = "INSERT INTO `$tableName` (`className`, `dataSource`, `metaData`) VALUES (?, ?, ?)";
                    $insertStmt = $dbConnection->prepare($insertQuery);
                    $insertStmt->bind_param("sss", $className, $dataSource, $metaData);
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
                throw $e;
            }
        } else {
            throw new Exception("Invalid data structure.");
        }
    }
}

function upsertReservationStay($data, $dbConnection) {
    $tableName = 'RESERVATIONstay';

    foreach ($data as $element) {
        // Convert ISO 8601 datetime to Unix timestamp
        $createDateTime = strtotime($element['createDateTime']) ?: null;
        $modifyDateTime = strtotime($element['modifyDateTime']) ?: null;
        $startDate = $element['startDate'] ?? null;
        $endDate = $element['endDate'] ?? null;
        $extPMSConfNum = $element['extPMSConfNum'] ?? null;
        $dataSource = $element['dataSource'] ?? null;
        $libSourceId = $element['libSourceId'] ?? null;
        $libPropertyId = $element['libPropertyId'] ?? null;

        $dbConnection->begin_transaction();

        try {
            // Check if a record with this combination already exists
            $checkQuery = "SELECT `id` FROM `$tableName` WHERE `createDateTime` = FROM_UNIXTIME(?) AND `modifyDateTime` = FROM_UNIXTIME(?) AND `startDate` = ? AND `endDate` = ? AND `extPMSConfNum` = ?";
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
                $updateQuery = "UPDATE `$tableName` SET `extPMSConfNum` = ?, `dataSource` = ?, `libSourceId` = ?, `libPropertyId` = ? WHERE `createDateTime` = FROM_UNIXTIME(?) AND `modifyDateTime` = FROM_UNIXTIME(?) AND `startDate` = ? AND `endDate` = ? AND `extPMSConfNum` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                if (!$updateStmt) {
                    throw new Exception("Prepare failed: " . $dbConnection->error);
                }

                $updateStmt->bind_param("ssiiissss", $extPMSConfNum, $dataSource, $libSourceId, $libPropertyId, $createDateTime, $modifyDateTime, $startDate, $endDate, $extPMSConfNum);
                $updateStmt->execute();
                if ($updateStmt->error) {
                    throw new Exception("Error in update operation: " . $updateStmt->error);
                }
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`createDateTime`, `modifyDateTime`, `startDate`, `endDate`, `extPMSConfNum`, `dataSource`, `libSourceId`, `libPropertyId`) VALUES (FROM_UNIXTIME(?), FROM_UNIXTIME(?), ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                if (!$insertStmt) {
                    throw new Exception("Prepare failed: " . $dbConnection->error);
                }

                $insertStmt->bind_param("iissssii", $createDateTime, $modifyDateTime, $startDate, $endDate, $extPMSConfNum, $dataSource, $libSourceId, $libPropertyId);
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
            error_log("Upsert failed: " . $e->getMessage(), 3, 'error_log.txt');
            throw $e;
        }
    }
}

function upsertCustomerRelationship($data, $dbConnection) {
    $tableName = 'CUSTOMERrelationship';

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
            // Rollback the transaction on error
            $dbConnection->rollback();
            throw $e;
        }
    }
}

function upsertCustomerMembership($data, $dbConnection) {
    $tableName = 'CUSTOMERmembership';

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
            // Rollback the transaction on error
            $dbConnection->rollback();
            throw $e;
        }
    }
}

function upsertServicesPayment($data, $dbConnection) {
    $tableName = 'SERVICESpayment';

    foreach ($data as $element) {
        $paymentAmount = $element['paymentAmount'] ?? null;
        $currencyCode = $element['currencyCode'] ?? null;
        $dataSource = $element['dataSource'] ?? null;
        $libTenderId = $element['libTenderId'] ?? null;

        // Start a transaction
        $dbConnection->begin_transaction();

        try {
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
                $insertStmt->bind_param("dssi", $paymentAmount, $currencyCode, $dataSource, $libTenderId);
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
            throw $e;
        }
    }
}



//take table from relational database and convert it into an associative array. Using this method to get around how the
//ID field is generated table-side and not easily predictable since it's generated at the server level instead of table
//level
function getTableAsAssociativeArray($connection, $tableName) {
    // Ensure the table name is safe to use in a query
    $tableName = mysqli_real_escape_string($connection, $tableName);

    // Query to get all rows from the table
    $query = "SELECT * FROM `$tableName`";
    $result = mysqli_query($connection, $query);

    // Check for a valid result
    if (!$result) {
        die('Query failed: ' . mysqli_error($connection));
    }

    // Create an associative array to store table data
    $tableData = [];

    // Fetch each row as an associative array
    while ($row = mysqli_fetch_assoc($result)) {
        $tableData[] = $row;
    }

    // Free result set
    mysqli_free_result($result);

    return $tableData;
}


//create the arrRESERVATIONstay array by parsing from $myDataSemiParsed
function createArrReservationStay(
    $connection,
    $myDataSemiParsed,
    $arrRESERVATIONlibSource,
    $arrRESERVATIONlibProperty
) {
    $arrRESERVATIONStay = [];

    // Create lookup arrays for source and property Ids
    $sourceLookup = createLookup($connection, 'RESERVATIONlibSource', 'sourceName', 'sourceType');
    $propertyLookup = createLookup($connection, 'RESERVATIONlibProperty','propertyCode', 'chainCode');

    foreach ($myDataSemiParsed as $entry) {
        $profiles = json_decode($entry['profiles'], true) ?? [];
        $profileData = $profiles[0] ?? []; // Assuming the first profile is relevant
        $sourceName = $profileData['names'][0]['name'] ?? 'UNKNOWN';
        $sourceType = $profileData['type'] ?? 'UNKNOWN';
        $propertyCode = $entry['extracted_property_code'] ?? 'UNKNOWN';
        $chainCode = $entry['extracted_chain_code'] ?? 'UNKNOWN';
        $propertyCode = $entry['extracted_property_code'] ?? 'UNKNOWN';
        $chainCode = $entry['extracted_chain_code'] ?? 'UNKNOWN';

        // Populate libSourceId and libPropertyId based on lookup
        $libSourceId = $sourceLookup[$sourceName][$sourceType] ?? null;
        $libPropertyId = $propertyLookup[$propertyCode][$chainCode] ?? null;
        $arrRESERVATIONStay[] = [
            'createDateTime' => $entry['createdDateTime'] ?? null,
            'modifyDateTime' => $entry['lastModifiedDateTime'] ?? null,
            'startDate' => $entry['arrival'] ?? null,
            'endDate' => $entry['departure'] ?? null,
            'createdBy' => $entry['createdBy'] ?? null,
            'metaData' => null,
            'extPMSConfNum' => $entry['confirmation_number'] ?? null,
            'extGuestId' => 'populated via trigger',
            'dataSource' => 'HAPI', // Assuming 'HAPI' is constant
            'libSourceId' => $libSourceId,
            'libPropertyId' => $libPropertyId,
            'propertyCode' => $propertyCode,
            'chainCode' => $chainCode,
            'sourceName' => $sourceName,
            'sourceType' => $sourceType,
        ];
    }
    // return $propertyLookup;
    return $arrRESERVATIONStay;
}


//create the arrCUSTOMERrelationship array by parsing from $myDataSemiParsed
function createArrCUSTOMERrelationship($myDataSemiParsed, $arrCUSTOMERlibContactType, $arrCUSTOMERcontact) {
    $arrCUSTOMERrelationship = [];

    // Create a lookup for contact types
    $contactTypeLookup = [];
    foreach ($arrCUSTOMERlibContactType as $type) {
        $contactTypeLookup[$type['type']] = $type['id'];
    }

    // Create a lookup for contacts
    $contactLookup = [];
    foreach ($arrCUSTOMERcontact as $contact) {
        $key = strtolower($contact['firstName'] . $contact['lastName'] . $contact['extGuestID']); // using lower case for case-insensitive comparison
        $contactLookup[$key] = $contact['id'];
    }

    foreach ($myDataSemiParsed as $entry) {
        $guestsData = json_decode($entry['guests'], true) ?? [];
        foreach ($guestsData as $guestData) {
            $guestInfo = $guestData['guest'] ?? null;
            if ($guestInfo) {
                // Translate isPrimary from true/false to 1/0
                $isPrimary = isset($guestData['isPrimary']) ? (int)$guestData['isPrimary'] : 0;
                $firstName = $guestInfo['names'][0]['givenName'] ?? null;
                $lastName = $guestInfo['names'][0]['surname'] ?? null;
                $extGuestId = $entry['extracted_guest_id'] ?? ''; // Assuming there's an identifier field with id

                // Generate lookup keys
                $contactKey = strtolower($firstName . $lastName . $extGuestId);

                // Lookup contactTypeId and contactId
                $contactTypeId = $contactTypeLookup['GUEST'] ?? null;
                $contactId = $contactLookup[$contactKey] ?? null;

                // Only add to the array if we have valid IDs
                if ($contactTypeId !== null && $contactId !== null) {
                    $arrCUSTOMERrelationship[] = [
                        'isPrimaryGuest' => $isPrimary,
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

    return $arrCUSTOMERrelationship;
}



// Assuming getContactIdFromDB is a function that retrieves the contact ID from CUSTOMERcontact table
function getContactIdFromDB($dbConnection, $firstName, $lastName, $extGuestId) {
    // Initialize the ID to null
    $contactId = null;

    // Prepare the SQL statement to prevent SQL injection
    $stmt = $dbConnection->prepare("SELECT `id` FROM `CUSTOMERcontact` WHERE `firstName` = ? AND `lastName` = ? AND `extGuestId` = ? LIMIT 1");

    // Bind the parameters
    $stmt->bind_param("sss", $firstName, $lastName, $extGuestId);

    // Execute the query
    if ($stmt->execute()) {
        // Bind the result
        $stmt->bind_result($contactId);

        // Fetch the result. If there's a result, $contactId will be set.
        $stmt->fetch();
    }

    // Close the statement
    $stmt->close();

    // Return the found ID or null
    return $contactId;
}



function createLookup($mysqli, $tableName, $keyField1, $keyField2) {
    $lookup = [];
    $query = "SELECT id, $keyField1, $keyField2 FROM $tableName";
    
    if ($result = $mysqli->query($query)) {
        while ($row = $result->fetch_assoc()) {
            $key1 = $row[$keyField1] ?? 'UNKNOWN';
            $key2 = $row[$keyField2] ?? 'UNKNOWN';
            $lookup[$key1][$key2] = $row['id'];
        }
        $result->free();
    }
    return $lookup;
}






//create the arrCUSTOMERmembership array by parsing from $myDataSemiParsed
function createArrCUSTOMERmembership($myDataSemiParsed, $arrCUSTOMERlibLoyaltyProgram, $arrCUSTOMERcontact) {
    $arrCUSTOMERmembership = [];
    $defaultContactId = array_column($arrCUSTOMERcontact, 'id', 'extGuestID')['UNKNOWN'];
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
            $extGuestId = $entry['extracted_guest_id'] ?? null;

            foreach ($memberships as $membership) {
                // Only update if membershipCode is not null or empty
                if (!empty($membership['membershipCode'])) {
                    $libLoyaltyProgramId = $loyaltyProgramIdForFairmont;
                } else {
                    $libLoyaltyProgramId = null; // or some default value you deem appropriate
                }

                // Set the contactId based on the guest data
                $contactId = $defaultContactId;
                foreach ($arrCUSTOMERcontact as $contact) {
                    if ($contact['extGuestID'] === $extGuestId && $contact['firstName'] === $firstName && $contact['lastName'] === $lastName) {
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

    return $arrCUSTOMERmembership;
}




//create the arrSERVICESpayment array by parsing from $myDataSemiParsed
function createArrSERVICESpayment($myDataSemiParsed, $arrSERVICESlibTender) {
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
        $paymentAmount = null; // As specified, always null
        $currencyCode = isset($entry['currency']) ? json_decode($entry['currency'], true)['code'] : null;
        $dataSource = 'HAPI'; // Constant value 'HAPI'
        $paymentMethod = isset($entry['paymentMethod']) ? json_decode($entry['paymentMethod'], true)['code'] : null;

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

    return $arrSERVICESpayment;
}

//create the arrRESERVATIONgroupStay array by parsing from $myDataSemiParsed
function create_arrRESERVATIONgroupStay($arrRESERVATIONstay, $arrRESERVATIONgroup) {
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
            'stayID' => '', // To be populated from arrRESERVATIONstay
            'startDate' => $item['arrival'],
            'endDate' => $item['departure'],
            'extPMSConfNum' => $item['extPMSConfNum'],
            'groupID' => '', // To be populated from arrRESERVATIONgroup
            'groupName' => 'UNKNOWN',
            'groupNumber' => 'UNKNOWN',
            'groupStartDate' => null,
            'groupEndDate' => null
        ];

        // Look up stayID
        $stayKey = $item['startDate'] . '|' . $item['endDate'] . '|' . $item['extPMSConfNum'];
        if (isset($stayLookup[$stayKey])) {
            $groupStayData['stayID'] = $stayLookup[$stayKey];
        }

        // Look up groupID
        foreach ($arrRESERVATIONgroup as $group) {
            if (
                $group['groupName'] === $groupStayData['groupName'] &&
                $group['groupNumber'] === $groupStayData['groupNumber'] &&
                $group['groupStartDate'] === $groupStayData['groupStartDate'] &&
                $group['groupEndDate'] === $groupStayData['groupEndDate']
            ) {
                $groupStayData['groupID'] = $group['id'];
                break;
            }
        }

        // Add the group stay data to the result array
        $arrRESERVATIONgroupStay[] = $groupStayData;
    }

    return $arrRESERVATIONgroupStay;
}

function createArrRESERVATIONstayStatusStay($myDataSemiParsed, $arrRESERVATIONstay, $arrRESERVATIONlibStayStatus) {
    $arrRESERVATIONstayStatusStay = [];

    foreach ($myDataSemiParsed as $entry) {
        // Look up the stayStatusID using statusName
        $stayStatusID = null;
        foreach ($arrRESERVATIONlibStayStatus as $status) {
            if ($status['statusName'] === $entry['ext_status'] ?? 'UNKNOWN') {
                $stayStatusID = $status['id'];
                break;
            }
        }

        // Look up the stayID using startDate, endDate, and extGuestID
        $stayID = null;
        foreach ($arrRESERVATIONstay as $stay) {
            if ($stay['startDate'] === $entry['arrival'] &&
                $stay['endDate'] === $entry['departure'] &&
                $stay['extGuestID'] === $entry['extracted_guest_id']) {
                $stayID = $stay['id'];
                break;
            }
        }

        // Create the array for this item
        $arrRESERVATIONstayStatusStay[] = [
            'cancelledBy' => $entry['cancellationDetails']['cancelledBy'] ?? null,
            'cancellationDateTime' => $entry['cancellationDetails']['cancellationDateTime'] ?? null,
            'cancellationReasonCode' => $entry['cancellationDetails']['cancellationReasonCode'] ?? null,
            'cancellationReasonText' => $entry['Cancellation']['cancellationReasonText'] ?? null,
            'dataSource' => 'HAPI', // Assuming 'HAPI' is a constant value
            'stayID' => $stayID,
            'startDate' => $entry['arrival'] ?? null,
            'endDate' => $entry['departure'] ?? null,
            'extGuestID' => $entry['extracted_guest_id'],
            'stayStatusID' => $stayStatusID,
            'statusName' => $entry['ext_status'] ?? 'UNKNOWN'
        ];
    }

    return $arrRESERVATIONstayStatusStay;
}

// Example usage:
// $arrRESERVATIONstayStatusStay = create_arrRESERVATIONstayStatusStay($myDataSemiParsed, $arrRESERVATIONstay, $arrRESERVATIONlibStayStatus);


//-------------------------------------------------------------------------------------------------------------------------

//Actual use of functions, or script logic:





//Pulling import code
$importCode = getFirstNonNullImportCode($originDBConnection, 'hapi_raw_reservations');


//Pull latest EtlTimestamp if exists from PMSDATABASEmisc
try {
    $etlStartTStamp = getLatestEtlTimestamp($destinationDBConnection);
}
catch (Exception $e)
{
echo 'Error: ' . $e->getMessage();
}

//fetch data from HAPI_RAW_RESERVATIONS and populate associative array
$insertCount = 0;
$updateCount = 0;
try {
    $myDataSemiParsed = fetchDataFromMySQLTable('hapi_raw_reservations', $originDBConnection, $destinationDBConnection, $insertCount, $updateCount);
}
catch (Exception $e)
{
    echo 'Error: ' . $e->getMessage();
}


try {
    insertEtlTrackingInfo($destinationDBConnection,$insertCount,$updateCount, $importCode, $schemaVersion);
}
catch (Exception $e)
{
    echo 'Error: ' . $e->getMessage();
}


//Parse out data from original data source array, $myDataSemiParsed, into arrays based on the final output tables
//// CUSTOMER
///PARENT
$arrCUSTOMERlibContactType = createCUSTOMERContactType();
$arrCUSTOMERContact = createCUSTOMERcontact($myDataSemiParsed);
$arrCUSTOMERlibLoyaltyProgram = createCUSTOMERloyaltyProgram();

//print_r($myDataSemiParsed)
/// CHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
/// GRANDCHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
///
//// SERVICES
///PARENT
$arrSERVICESlibFolioOrdersType = createSERVICESlibFolioOrderType();
$arrSERVICESlibTender = createSERVICESlibTender($myDataSemiParsed);
$arrSERVICESlibServiceItems = createSERVICESlibServiceItems($myDataSemiParsed);
/// CHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
/// GRANDCHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
///
//// RESERVATION
///PARENT
$arrRESERVATIONlibProperty = createRESERVATIONLibProperty($myDataSemiParsed);
$arrRESERVATIONlibSource = createRESERVATIONLibSource($myDataSemiParsed);
$arrRESERVATIONlibRoomClass = createRESERVATIONLibRoomClass($myDataSemiParsed);
$arrRESERVATIONlibRoomType = createRESERVATIONLibRoomType($myDataSemiParsed);
$arrRESERVATIONlibStayStatus = createRESERVATIONLibStayStatus($myDataSemiParsed);
$arrRESERVATIONGroup = createRESERVATIONGroup($myDataSemiParsed);
$arrRESERVATIONlibRoom = createRESERVATIONLibRoom($myDataSemiParsed);
/// CHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
/// GRANDCHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
///









//arrays of table names categorized by parent, child, and grandchild
$arrParentTables =
    ['CUSTOMERlibContactType',
    'CUSTOMERcontact',
    'CUSTOMERlibLoyaltyProgram',
    'SERVICESlibFolioOrderType',
    'SERVICESlibTender',
    'SERVICESlibServiceItems',
    'RESERVATIONlibProperty',
    'RESERVATIONlibSource',
    'RESERVATIONlibRoomClass',
    'RESERVATIONlibRoomType',
    'RESERVATIONlibStayStatus',
    'RESERVATIONgroup',
    'RESERVATIONlibRoom'];
$arrParentTableArrays =
    [
        $arrCUSTOMERlibContactType,
        $arrCUSTOMERContact,
        $arrCUSTOMERlibLoyaltyProgram,
        $arrSERVICESlibFolioOrdersType,
        $arrSERVICESlibTender,
        $arrSERVICESlibServiceItems,
        $arrRESERVATIONlibProperty,
        $arrRESERVATIONlibSource,
        $arrRESERVATIONlibRoomClass,
        $arrRESERVATIONlibRoomType,
        $arrRESERVATIONlibStayStatus,
        $arrRESERVATIONGroup,
        $arrRESERVATIONlibRoom

    ];
$childTables =
    ['RESERVATIONstay',
    'RESERVATIONroomDetails',
    'RESERVATIONstayStatuSstay',
    'CUSTOMERrelationship',
    'CUSTOMERmembership',
    'SERVICESpayment'];
$grandChildTables =
    ['RESERVATIONgroupstay',
    'SERVICESfolioOrders'];

//Validate Parent table associative arrays that they would fit into their appropriate tables
// Loop through each table and its corresponding data array


 //Upsert Parent table associative arrays into their appropriate tables

//Upsert into CUSTOMERlibContactType table
try {
   upsertCustomerContactType($arrCUSTOMERlibContactType, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into CUSTOMERcontact table
try {
   upsertCustomerContact($arrCUSTOMERContact, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into CUSTOMERlibLoyaltyProgram table
try {
   upsertCustomerLibLoyaltyProgram($arrCUSTOMERlibLoyaltyProgram, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into RESERVATIONlibRoom table
try {
   upsertReservationLibRoom($arrRESERVATIONlibRoom, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into RESERVATIONlibSource
try {
   upsertReservationLibSource($arrRESERVATIONlibSource, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibProperty table
try {
   upsertReservationLibProperty($arrRESERVATIONlibProperty, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into SERVICESlibTender table
try {
   upsertServicesLibTender($arrSERVICESlibTender, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into SERVICESlibServiceItems table
try {
   upsertServicesLibServiceItems($arrSERVICESlibServiceItems, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into SERVICESlibFolioOrdersType table
try {
   upsertServicesLibFolioOrdersType($arrSERVICESlibFolioOrdersType, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into RESERVATIONlibGroup table
try {
   upsertReservationGroup($arrRESERVATIONGroup, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibStayStatus table
try {
   upsertReservationLibStayStatus($arrRESERVATIONlibStayStatus, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibRoomType table
try {
   upsertReservationLibRoomType($arrRESERVATIONlibRoomType, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibRoomClass table
try {
   upsertReservationLibRoomClass($arrRESERVATIONlibRoomClass, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


// Get Parent table associative arrays  with new primary keys to prepare for upsert of child tables
// Update $arrCUSTOMERlibContactType
// $arrCUSTOMERlibContactType = updateArrayWithIdsForSpecificField($destinationDBConnection, $arrCUSTOMERlibContactType, 'CUSTOMERlibContactType', 'type');
$arrCUSTOMERlibContactType = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERlibContactType');
// Update $arrCUSTOMERContact
// $arrCUSTOMERContact = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrCUSTOMERContact, 'CUSTOMERcontact', ['firstName', 'lastName', 'extGuestId'],false);
$arrCUSTOMERContact = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERcontact');
// Update $arrCUSTOMERlibLoyaltyProgram
// $arrCUSTOMERlibLoyaltyProgram = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrCUSTOMERlibLoyaltyProgram, 'CUSTOMERlibLoyaltyProgram', ['Name', 'Source'],true);
$arrCUSTOMERlibLoyaltyProgram = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERlibLoyaltyProgram');
// Update $arrRESERVATIONlibRoom
// $arrRESERVATIONlibRoom = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrRESERVATIONlibRoom, 'RESERVATIONlibRoom', ['roomNumber'],false);
$arrRESERVATIONlibRoom = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibRoom');
//// Update $arrRESERVATIONlibRoomType
// $arrRESERVATIONlibRoomType = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrRESERVATIONlibRoomType, 'ReservationLibRoomType', ['typeCode'], true);
$arrRESERVATIONlibRoomType = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONLibRoomType');
//// Update $arrRESERVATIONlibRoomClass
// $arrRESERVATIONlibRoomClass = updateArrayWithIdsForMultipleFields($destinationDBConnection,  $arrRESERVATIONlibRoomClass,   'RESERVATIONlibRoomClass', ['className'],true);
$arrRESERVATIONlibRoomClass = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibRoomClass');
// Update $arrRESERVATIONlibProperty
// $arrRESERVATIONlibProperty = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrRESERVATIONlibProperty, 'RESERVATIONlibProperty',['propertyCode','chainCode'], false);
$arrRESERVATIONlibProperty = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibProperty');
// Update $arrRESERVATIONGroup
// $arrRESERVATIONGroup = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrRESERVATIONGroup, 'RESERVATIONgroup',['groupName', 'groupNumber'], true);
$arrRESERVATIONGroup = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONgroup');
// Update $arrRESERVATIONlibsource
// $arrRESERVATIONlibSource = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrRESERVATIONlibSource, 'RESERVATIONlibsource',['sourceName', 'sourceType'], true);
$arrRESERVATIONlibSource = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibsource');
//// Update $arrRESERVATIONlibstaystatus
// $arrRESERVATIONlibStayStatus = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrRESERVATIONlibStayStatus, 'RESERVATIONlibstaystatus',['statusName'], true);
$arrRESERVATIONlibStayStatus = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibstaystatus');
// Update $arrSERVICESlibtender
// $arrSERVICESlibTender = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrSERVICESlibTender, 'SERVICESlibTender',['paymentMethod'], false);
$arrSERVICESlibTender = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibTender');
// Update $arrSERVICESlibFolioOrdersType
// $arrSERVICESlibFolioOrdersType = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrSERVICESlibFolioOrdersType, 'SERVICESlibFolioOrdersType',['orderType'], true);
//$arrSERVICESlibFolioOrdersType = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibFolioOrdersType');
// Update $arrRESERVATIONlibProperty
// $arrSERVICESlibServiceItems = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrSERVICESlibServiceItems, 'SERVICESlibServiceItems',['itemName', 'itemCode', 'ratePlanCode'], false);
$arrSERVICESlibServiceItems = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibServiceItems');


//Child arrays and tables
// 1) RESERVATIONstay
// 2) RESERVATIONroomDetails
// 3) RESERVATIONstayStatusStay
// 4) CUSTOMERrelationship

// 5) CUSTOMERmembership
// 6) SERVICESpayment
//Create child associative arrays using the populated parent tables
// 1) RESERVATIONstay
$arrRESERVATIONstay = createArrReservationStay($destinationDBConnection,$myDataSemiParsed, $arrRESERVATIONlibSource, $arrRESERVATIONlibProperty);
// 2) CUSTOMERrelationship
$arrCUSTOMERrelationship = createArrCUSTOMERrelationship($myDataSemiParsed, $arrCUSTOMERlibContactType, $arrCUSTOMERContact);
// 3) CUSTOMERmembership
$arrCUSTOMERmembership = createArrCUSTOMERmembership($myDataSemiParsed, $arrCUSTOMERlibLoyaltyProgram, $arrCUSTOMERContact);
// 4) SERVICESpayment
$arrSERVICESpayment = createArrSERVICESpayment($myDataSemiParsed, $arrSERVICESlibTender);
//Populate child tables
// 1) RESERVATIONgroupStay
//Upsert into RESERVATIONstay table
try {
    upsertRESERVATIONstay($arrRESERVATIONstay, $destinationDBConnection);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
// 2) CUSTOMERrelationship
try {
    upsertCustomerRelationship($arrCUSTOMERrelationship, $destinationDBConnection);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
// 3) CUSTOMERmembership
try {
    upsertCUSTOMERmembership($arrCUSTOMERmembership, $destinationDBConnection);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
// 4) SERVICESpayment
try {
    upsertSERVICESpayment($arrSERVICESpayment, $destinationDBConnection);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}

// Get Child table associative arrays with new primary keys to prepare for upsert of grandchild tables
// Update $arrRESERVATIONstay
$arrRESERVATIONstay = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONstay');
// Update $arrCUSTOMERrelationship
$arrCUSTOMERrelationship = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERrelationship');
// Update $arrCUSTOMERmembership
$arrCUSTOMERmembership= getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERmembership');
// Update $arrSERVICESpayment
$arrSERVICESpayment = getTableAsAssociativeArray($destinationDBConnection,'SERVICESpayment');


// Grandchild tables
// 1) RESERVATIONroomDetails
// 2) RESERVATIONstayStatusStay
// 3) RESERVATIONgroupStay
// 4) SERVICESfolioOrders
//Create grandchild associative arrays using the populated parent tables
// 1) RESERVATIONroomDetails
//$arrRESERVATIONroomDetails = createArrReservationRoomDetails($myDataSemiParsed, $arrRESERVATIONlibRoom, $arrRESERVATIONstay, $arrRESERVATIONlibRoomType,$arrRESERVATIONlibRoomClass);
// 2) RESERVATIONstayStatusStay
$arrRESERVATIONstayStatusStay = createArrRESERVATIONstayStatusStay($myDataSemiParsed, $arrRESERVATIONstay, $arrRESERVATIONlibStayStatus);
// 3) RESERVATIONgroupStay
//May need to put this one on hold until we get actual group data from Hapi =/
//$arrRESERVATIONgroupStay = create_arrRESERVATIONgroupStay($arrRESERVATIONstay, $arrRESERVATIONgroup);
// 4) SERVICESfolioOrders
//Populate grandchild tables
// 1) RESERVATIONroomDetails
// 2) RESERVATIONstayStatusStay
// 3) RESERVATIONgroupStay
// 4) SERVICESfolioOrders
// Get Grandchild table associative arrays with new primary keys
// 1) RESERVATIONroomDetails
// 2) RESERVATIONstayStatusStay
// 3) RESERVATIONgroupStay
// 4) SERVICESfolioOrders

//Populate grandchild tables
var_dump(array_slice($arrRESERVATIONstay, 0, 10, true));
var_dump(array_slice($arrRESERVATIONstayStatusStay, 0, 10, true));
//var_dump($arrRESERVATIONstayStatusStay);
//print_r($arrRESERVATIONstayStatusStay);
//print_r($arrRESERVATIONroomDetails);
//var_dump(array_slice($arrCUSTOMERlibLoyaltyProgram, 0, 10, true));
//var_dump(array_slice($arrSERVICESpayment, 0, 10, true));
//print($etlStartTStamp);
//echo "\n";
//print($importCode);
//echo "\n";
//print($insertCount);
//echo "\n";
//print($updateCount);
//echo "\n";
//print(getLatestEtlTimestamp($destinationDBConnection));
//var_dump(array_slice($arrCUSTOMERlibContactType, 0, 10, true));
// var_dump(parseAndFlattenArray($myDataSemiParsed));
// print_r($arrRESERVATIONlibSource);
// var_dump($arrRESERVATIONlibProperty);
// var_dump($arrRESERVATIONlibProperty);

//// Check for null foreign keys
//// Assuming $arr is your associative array
//$arr = $arrCUSTOMERrelationship; // replace this with your actual array
//
//$contactTypeIdCounts = [];
//$contactIdCounts = [];
//
//foreach ($arr as $item) {
//    // Count for contactTypeId
//    if (isset($item['contactTypeId'])) {
//        $contactTypeId = $item['contactTypeId'];
//        if (!isset($contactTypeIdCounts[$contactTypeId])) {
//            $contactTypeIdCounts[$contactTypeId] = 0;
//        }
//        $contactTypeIdCounts[$contactTypeId]++;
//    }
//
//    // Count for contactId
//    if (isset($item['contactId'])) {
//        $contactId = $item['contactId'];
//        if (!isset($contactIdCounts[$contactId])) {
//            $contactIdCounts[$contactId] = 0;
//        }
//        $contactIdCounts[$contactId]++;
//    }
//}
//
//// Output the results
//echo "Counts for each contactTypeId:\n";
//foreach ($contactTypeIdCounts as $contactTypeId => $count) {
//    echo "contactTypeId: " . $contactTypeId . " - Count: " . $count . "\n";
//}
//
//echo "\nCounts for each contactId:\n";
//foreach ($contactIdCounts as $contactId => $count) {
//    echo "contactId: " . $contactId . " - Count: " . $count . "\n";
//}



try {
    updateEtlDuration($destinationDBConnection);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
?>
