<?php


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
function fetchDataFromMySQLTable($tableName, $originDBConnection)
{
    try {
        // Check connection
        if ($originDBConnection->connect_error) {
            throw new Exception('Connection failed: ' . $originDBConnection->connect_error);
        }

        // Start transaction
        $originDBConnection->begin_transaction();

        // Prepare and execute query
        $query = "SELECT * FROM $tableName";
        $result = $originDBConnection->query($query);

        // Check for query success
        if (!$result) {
            throw new Exception('Query failed: ' . $originDBConnection->error);
        }

        // Fetch data and store it in an array
        $data = [];
        while ($row = $result->fetch_assoc()) {
            $data[] = $row;
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
    $contactTypeArray = [
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

    return $contactTypeArray;
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
            'extGuestID' => 'UNKNOWN',
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
                    'extGuestID' => $item['extracted_guest_id'] ?? '',
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
            'extGuestID' => ['type' => 'varchar', 'length' => 45],
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
        $extGuestID = $record['extGuestID'] ?? null;

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
            $checkQuery = "SELECT COUNT(*) FROM `$tableName` WHERE `firstName` = ? AND `lastName` = ? AND `extGuestID` = ?";
            $stmt = $dbConnection->prepare($checkQuery);
            $stmt->bind_param("sss", $firstName, $lastName, $extGuestID);
            $stmt->execute();
            $result = $stmt->get_result();
            $exists = $result->fetch_row()[0] > 0;

            // Upsert query
            if ($exists) {
                // Update
                $updateQuery = "UPDATE `$tableName` SET `title` = ?, `email` = ?, `birthDate` = ?, `languageCode` = ?, `languageFormat` = ?, `metaData` = ?, `dataSource` = ? WHERE `firstName` = ? AND `lastName` = ? AND `extGuestID` = ?";
                $updateStmt = $dbConnection->prepare($updateQuery);
                $updateStmt->bind_param("ssssssssss", $title, $email, $birthDate, $languageCode, $languageFormat, $metaData, $dataSource, $firstName, $lastName, $extGuestID);
            } else {
                // Insert
                $insertQuery = "INSERT INTO `$tableName` (`firstName`, `lastName`, `title`, `email`, `birthDate`, `languageCode`, `languageFormat`, `metaData`, `dataSource`, `extGuestID`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                $insertStmt = $dbConnection->prepare($insertQuery);
                $insertStmt->bind_param("ssssssssss", $firstName, $lastName, $title, $email, $birthDate, $languageCode, $languageFormat, $metaData, $dataSource, $extGuestID);
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
        if (isset($element['SERVICESlibFolioOrdersType'])) {
            $record = $element['SERVICESlibFolioOrdersType'];

            // Extract the required fields
            $orderType = $record['type'] ?? null;
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

function findIdInDatabase($mysqli, $tableName, $searchCriteria) {
    $query = "SELECT id FROM $tableName WHERE ";
    $conditions = [];
    $params = [];
    $types = '';

    foreach ($searchCriteria as $key => $value) {
        $conditions[] = "$key = ?";
        $params[] = &$searchCriteria[$key];
        $types .= 's';
    }

    $query .= implode(' AND ', $conditions);

    $stmt = $mysqli->prepare($query);

    if (!$stmt) {
        die("Error preparing query: " . $mysqli->error);
    }

    $stmt->bind_param($types, ...$params);
    $stmt->execute();
    $result = $stmt->get_result();
    $row = $result->fetch_assoc();


    $stmt->close();

    return $row ? $row['id'] : null;
}

function updateArrayWithIdsForSpecificField($mysqli, $arr, $tableName, $matchingField) {
    foreach ($arr as $key => $value) {
        // Adjusted to access the nested 'CUSTOMERlibContactType' key
        if (isset($value['CUSTOMERlibContactType'][$matchingField])) {
            $searchCriteria = [$matchingField => $value['CUSTOMERlibContactType'][$matchingField]];
            $id = findIdInDatabase($mysqli, $tableName, $searchCriteria);
            $arr[$key]['id'] = $id; // Add the id to the top level of each element
        } else {
            // Handle the case where the matching field is not set
            $arr[$key]['id'] = null;
            echo "Warning: '$matchingField' key not found for element at index $key.\n";
        }
    }
    return $arr;
}

function updateArrayWithIdsForMultipleFields($mysqli, $arr, $tableName, $matchingFields, $isNested = false) {
    foreach ($arr as $key => $value) {
        $searchCriteria = [];

        // Adjust data access based on whether the data is nested
        $data = $isNested ? $value[$tableName] : $value;

        foreach ($matchingFields as $field) {
            if (isset($data[$field])) {
                $searchCriteria[$field] = $data[$field];
            } else {
                // Handle the case where a matching field is not set
                echo "Warning: '$field' key not found for element at index $key.\n";
                $searchCriteria[$field] = null; // or handle differently as needed
            }
        }

        $id = findIdInDatabase($mysqli, $tableName, $searchCriteria);
        $arr[$key]['id'] = $id; // Add the id to the top level of each element
    }
    return $arr;
}

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


function createArrReservationStay(
    $connection,
    $myDataSemiParsed, 
    $arrRESERVATIONlibSource, 
    $arrRESERVATIONlibProperty
) {
    $arrRESERVATIONStay = [];

    // Create lookup arrays for source and property IDs
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
    
        // Populate libSourceID and libPropertyID based on lookup
        $libSourceID = $sourceLookup[$sourceName][$sourceType] ?? null;
        $libPropertyID = $propertyLookup[$propertyCode][$chainCode] ?? null;
        $arrRESERVATIONStay[] = [
            'createDateTime' => $entry['createdDateTime'] ?? null,
            'modifyDateTime' => $entry['lastModifiedDateTime'] ?? null,
            'startDate' => $entry['arrival'] ?? null,
            'endDate' => $entry['departure'] ?? null,
            'createdBy' => $entry['createdBy'] ?? null,
            'metaData' => null,
            'extPMSConfNum' => $entry['confirmation_number'] ?? null, 
            'extGuestID' => 'populated via trigger',
            'dataSource' => 'HAPI', // Assuming 'HAPI' is constant
            'libSourceID' => $libSourceID,
            'libPropertyID' => $libPropertyID,
            'propertyCode' => $propertyCode,
            'chainCode' => $chainCode,
            'sourceName' => $sourceName,
            'sourceType' => $sourceType,
        ];
    }
    // return $propertyLookup;
    return $arrRESERVATIONStay;
}
// function createArrReservationStay(
//     $myDataSemiParsed, 
//     $arrRESERVATIONlibSource, 
//     $arrRESERVATIONlibProperty
// ) {
//     $arrRESERVATIONStay = [];
//     $existingCombos = [];

//     // Prepare a lookup array for source and property IDs
//     $sourceLookup = createLookup($arrRESERVATIONlibSource, 'sourceName', 'sourceType');
//     $propertyLookup = createLookup($arrRESERVATIONlibProperty, 'propertyCode', 'chainCode');

//     foreach ($myDataSemiParsed as $entry) {
//         $profiles = json_decode($entry['profiles'], true) ?? [];
//         $profileData = $profiles[0] ?? []; // Assuming the first profile is what we want
//         $sourceName = $profileData['names'][0]['name'] ?? 'UNKNOWN';
//         $sourceType = $profileData['type'] ?? 'UNKNOWN';
//         $propertyCode = $entry['extracted_property_code'] ?? 'UNKNOWN';
//         $chainCode = $entry['extracted_chain_code'] ?? 'UNKNOWN';

//         // Unique combination check to avoid duplicates
//         $combo = $sourceName . $sourceType . $propertyCode . $chainCode;
//         if (!in_array($combo, $existingCombos)) {
//             $existingCombos[] = $combo;

//             $arrRESERVATIONStay[] = [
//                 'createDateTime' => $entry['createdDateTime'] ?? null,
//                 'modifyDateTime' => $entry['lastModifiedDateTime'] ?? null,
//                 'startDate' => $entry['arrival'] ?? null,
//                 'endDate' => $entry['departure'] ?? null,
//                 'createdBy' => $entry['createdBy'] ?? null,
//                 'extGuestID' => null, // Assuming this is still null
//                 'dataSource' => 'HAPI', // Assuming 'HAPI' is constant
//                 'libSourceID' => $sourceLookup[$sourceName][$sourceType] ?? null,
//                 'libPropertyID' => $propertyLookup[$propertyCode][$chainCode] ?? null,
//                 'propertyCode' => $propertyCode,
//                 'chainCode' => $chainCode,
//                 'sourceName' => $sourceName,
//                 'sourceType' => $sourceType,
//             ];
//         }
//     }

//     return $propertyLookup;
// }


// function createLookup($array, $keyField1, $keyField2) {
//     $lookup = [];
//     foreach ($array as $item) {
//         $key1 = findInNestedArray($item, $keyField1);
//         $key2 = findInNestedArray($item, $keyField2);
//         if ($key1 !== null && $key2 !== null) {
//             $lookup[$key1][$key2] = $item['id'];
//         }
//     }
//     return $lookup;
// }

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




function createArrCUSTOMERrelationship($myDataSemiParsed) {
    $arrCUSTOMERrelationship = [];

    foreach ($myDataSemiParsed as $entry) {
        $guestsData = json_decode($entry['guests'], true) ?? [];
        foreach ($guestsData as $guestData) {
            $guestInfo = $guestData['guest'] ?? null;
            if ($guestInfo) {
                $isPrimary = isset($guestData['isPrimary']) ? (int)$guestData['isPrimary'] : 0; // Convert true/false to 1/0
                $relationship = [
                    'isPrimaryGuest' => $isPrimary,
                    'contactTypeID' => null, // Foreign key to be linked later
                    'type' => 'GUEST',
                    'contactID' => null, // Foreign key to be linked later
                    'firstName' => $guestInfo['names'][0]['givenName'] ?? null,
                    'LastName' => $guestInfo['names'][0]['surname'] ?? null,
                    'dataSource' => 'HAPI'
                ];
                $arrCUSTOMERrelationship[] = $relationship;
            }
        }
    }

    return $arrCUSTOMERrelationship;
}

function createArrCUSTOMERmembership($myDataSemiParsed) {
    $arrCUSTOMERmembership = [];

    foreach ($myDataSemiParsed as $entry) {
        // Decode the memberships JSON string into an associative array
        $memberships = json_decode($entry['memberships'], true) ?? [];
        // Decode the guests JSON string to get the first guest's data
        $guestData = json_decode($entry['guests'], true)[0]['guest'] ?? null;

        // If there are memberships and guest data, process them
        if (!empty($memberships) && $guestData) {
            // Extract first and last name from the guest data
            $firstName = $guestData['names'][0]['givenName'] ?? null;
            $lastName = $guestData['names'][0]['surname'] ?? null;

            // Process each membership entry
            foreach ($memberships as $membership) {
                // Create the customer membership array
                $arrCUSTOMERmembership[] = [
                    'level' => $membership['level'] ?? null,
                    'membershipCode' => $membership['membershipCode'] ?? null,
                    'dataSource' => 'HAPI',
                    'libLoyaltyProgramID' => null, // To be connected with foreign key
                    'Name' => 'Fairmont Banff Springs',
                    'Source' => 'Fairmont Banff Springs',
                    'contactID' => null, // To be connected with foreign key
                    'firstName' => $firstName,
                    'LastName' => $lastName,
                    'extGuestID' => $entry['extracted_guest_id'] ?? null,
                ];
            }
        }
    }

    return $arrCUSTOMERmembership;
}

function createArrSERVICESpayment($myDataSemiParsed) {
    $arrSERVICESpayment = [];

    foreach ($myDataSemiParsed as $entry) {
        // Initialize default values
        $paymentAmount = null; // As specified, always null
        $currencyCode = isset($entry['currency']) ? json_decode($entry['currency'], true)['code'] : null;
        $dataSource = 'HAPI'; // Constant value 'HAPI'
        $libTenderID = null; // Foreign key, left null for now
        $paymentMethod = isset($entry['paymentMethod']) ? json_decode($entry['paymentMethod'], true)['code'] : null;

        // Construct the associative array
        $paymentData = [
            'paymentAmount' => $paymentAmount,
            'currencyCode' => $currencyCode,
            'dataSource' => $dataSource,
            'libTenderID' => $libTenderID,
            'paymentMethod' => $paymentMethod
        ];

        $arrSERVICESpayment[] = $paymentData;
    }

    return $arrSERVICESpayment;
}

// Usage:
// $myDataSemiParsed = /* Your data array */;
// $arrSERVICESpayment = createArrSERVICESpayment($myDataSemiParsed);





//create an array with all data from original data source
try {
    $myDataSemiParsed = fetchDataFromMySQLTable($originTableName, $originDBConnection);
    // Work with $data...
} catch (Exception $e) {
    // Handle exception if needed
    echo 'Error: ' . $e->getMessage();
}

//Parse out data from original data source array, $myDataSemiParsed, into arrays based on the final output tables
//// CUSTOMER
///PARENT
$arrCustomerlibContactType = createCUSTOMERContactType();
$arrCustomerContact = createCUSTOMERcontact($myDataSemiParsed);
$arrCustomerlibLoyaltyProgram = createCUSTOMERloyaltyProgram();
//print_r($myDataSemiParsed)
/// CHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
/// GRANDCHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
///
//// SERVICES
///PARENT
$arrServiceslibFolioOrderType = createSERVICESlibFolioOrderType();
$arrServiceslibTender = createSERVICESlibTender($myDataSemiParsed);
$arrServiceslibServiceItems = createSERVICESlibServiceItems($myDataSemiParsed);
/// CHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
/// GRANDCHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
///
//// RESERVATION
///PARENT
$arrReservationlibProperty = createRESERVATIONLibProperty($myDataSemiParsed);
$arrReservationlibSource = createRESERVATIONLibSource($myDataSemiParsed);
$arrReservationlibRoomClass = createRESERVATIONLibRoomClass($myDataSemiParsed);
$arrReservationlibRoomType = createRESERVATIONLibRoomType($myDataSemiParsed);
$arrReservationlibStayStatus = createRESERVATIONLibStayStatus($myDataSemiParsed);
$arrReservationGroup = createRESERVATIONGroup($myDataSemiParsed);
$arrReservationlibRoom = createRESERVATIONLibRoom($myDataSemiParsed);
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
        $arrCustomerlibContactType,
        $arrCustomerContact,
        $arrCustomerlibLoyaltyProgram,
        $arrServiceslibFolioOrderType,
        $arrServiceslibTender,
        $arrServiceslibServiceItems,
        $arrReservationlibProperty,
        $arrReservationlibSource,
        $arrReservationlibRoomClass,
        $arrReservationlibRoomType,
        $arrReservationlibStayStatus,
        $arrReservationGroup,
        $arrReservationlibRoom

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


// //Upsert Parent table associative arrays into their appropriate tables

//Upsert into CUSTOMERlibContactType table
try {
   upsertCustomerContactType($arrCustomerlibContactType, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into CUSTOMERcontact table
try {
   upsertCustomerContact($arrCustomerContact, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into CUSTOMERlibLoyaltyProgram table
try {
   upsertCustomerLibLoyaltyProgram($arrCustomerlibLoyaltyProgram, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into RESERVATIONlibRoom table
try {
   upsertReservationLibRoom($arrReservationlibRoom, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into RESERVATIONlibSource
try {
   upsertReservationLibSource($arrReservationlibSource, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibProperty table
try {
   upsertReservationLibProperty($arrReservationlibProperty, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into SERVICESlibTender table
try {
   upsertServicesLibTender($arrServiceslibTender, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into SERVICESlibServiceItems table
try {
   upsertServicesLibServiceItems($arrServiceslibServiceItems, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into SERVICESlibFolioOrdersType table
try {
   upsertServicesLibFolioOrdersType($arrServiceslibFolioOrderType, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into RESERVATIONgroupFolioOrdersType table
try {
   upsertReservationGroup($arrReservationGroup, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibStayStatus table
try {
   upsertReservationLibStayStatus($arrReservationlibStayStatus, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibRoomType table
try {
   upsertReservationLibRoomType($arrReservationlibRoomType, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibRoomClass table
try {
   upsertReservationLibRoomClass($arrReservationlibRoomClass, $destinationDBConnection);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}



// Update $arrCustomerlibContactType
// $arrCustomerlibContactType = updateArrayWithIdsForSpecificField($destinationDBConnection, $arrCustomerlibContactType, 'CUSTOMERlibContactType', 'type');
$arrCustomerlibContactType = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERlibContactType');
// Update $arrCustomerContact
// $arrCustomerContact = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrCustomerContact, 'CUSTOMERcontact', ['firstName', 'lastName', 'extGuestID'],false);
$arrCustomerContact = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERcontact');
// Update $arrCustomerlibLoyaltyProgram
// $arrCustomerlibLoyaltyProgram = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrCustomerlibLoyaltyProgram, 'CUSTOMERlibLoyaltyProgram', ['Name', 'Source'],true);
$arrCustomerlibLoyaltyProgram = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERlibLoyaltyProgram');
// Update $arrReservationlibRoom
// $arrReservationlibRoom = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrReservationlibRoom, 'RESERVATIONlibRoom', ['roomNumber'],false);
$arrReservationlibRoom = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibRoom');
//// Update $arrReservationlibRoomType
// $arrReservationlibRoomType = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrReservationlibRoomType, 'ReservationLibRoomType', ['typeCode'], true);
$arrReservationlibRoomType = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONLibRoomType');
//// Update $arrReservationlibRoomClass
// $arrReservationlibRoomClass = updateArrayWithIdsForMultipleFields($destinationDBConnection,  $arrReservationlibRoomClass,   'RESERVATIONlibRoomClass', ['className'],true);
$arrReservationlibRoomClass = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibRoomClass');
// Update $arrRESERVATIONlibProperty
// $arrReservationlibProperty = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrReservationlibProperty, 'RESERVATIONlibProperty',['propertyCode','chainCode'], false);
$arrReservationlibProperty = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibProperty');
// Update $arrReservationGroup
// $arrReservationGroup = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrReservationGroup, 'RESERVATIONgroup',['groupName', 'groupNumber'], true);
$arrReservationGroup = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONgroup');
// Update $arrRESERVATIONlibsource
// $arrReservationlibSource = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrReservationlibSource, 'RESERVATIONlibsource',['sourceName', 'sourceType'], true);
$arrReservationlibSource = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibsource');
//// Update $arrRESERVATIONlibstaystatus
// $arrReservationlibStayStatus = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrReservationlibStayStatus, 'RESERVATIONlibstaystatus',['statusName'], true);
$arrReservationlibStayStatus = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibstaystatus');
// Update $arrSERVICESlibtender
// $arrServiceslibTender = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrServiceslibTender, 'SERVICESlibTender',['paymentMethod'], false);
$arrServiceslibTender = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibTender');
// Update $arrServiceslibFolioOrderType
// $arrServiceslibFolioOrderType = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrServiceslibFolioOrderType, 'SERVICESlibFolioOrdersType',['orderType'], true);
$arrServiceslibFolioOrderType = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibFolioOrdersType');
// Update $arrRESERVATIONlibProperty
// $arrServiceslibServiceItems = updateArrayWithIdsForMultipleFields($destinationDBConnection, $arrServiceslibServiceItems, 'SERVICESlibServiceItems',['itemName', 'itemCode', 'ratePlanCode'], false);
$arrServiceslibServiceItems = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibServiceItems');


//Child arrays and tables
// 1) RESERVATIONstay
// 2) RESERVATIONroomDetails
// 3) RESERVATIONstayStatusStay
// 4) CUSTOMERrelationship

// 5) CUSTOMERmembership
// 6) SERVICESpayment
//Create child associative arrays using the populated parent tables
// 1) RESERVATIONstay
$arrRESERVATIONstay = createArrReservationStay($destinationDBConnection,$myDataSemiParsed, $arrReservationlibSource, $arrRESERVATIONlibProperty);

// 2) CUSTOMERrelationship
$arrCUSTOMERrelationship = createArrCUSTOMERrelationship($myDataSemiParsed);
// 3) CUSTOMERmembership
$arrCUSTOMERmembership = createArrCUSTOMERmembership($myDataSemiParsed);
// 4) SERVICESpayment
$arrSERVICESpayment = createArrSERVICESpayment($myDataSemiParsed);
//Populate child tables
// 1) RESERVATIONgroupStay
// 2) CUSTOMERrelationship9
// 3) CUSTOMERmembership
// 4) SERVICESpayment
// Grandchild tables
// 1) RESERVATIONroomDetails
// 2) RESERVATIONstayStatusStay
// 3) RESERVATIONgroupStay
// 4) SERVICESfolioOrders
//Grandchild arrays and tables
// 1) RESERVATIONgroupStay
// 2) SERVICESfolioOrders
//create grandchild associative arrays
// 1) RESERVATIONgroupStay
// 2) SERVICESfolioOrders

//Populate grandchild tables

print_r($arrRESERVATIONstay);
// var_dump(parseAndFlattenArray($myDataSemiParsed));
// print_r($arrReservationlibSource);
// var_dump($arrReservationlibProperty);
// var_dump($arrReservationlibProperty);
?>
