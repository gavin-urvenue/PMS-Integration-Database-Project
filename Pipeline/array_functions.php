<?php
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
    $errorLogFile = 'error_log.txt'; // Define the error log file path

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
        $errorTimestamp = date('Y-m-d H:i:s'); // Format the date and time as you prefer
        $errorLogMessage = "[{$errorTimestamp}] fetchDataFromMySQLTable failed: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
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
        $errorTimestamp = date('Y-m-d H:i:s'); // Format the date and time as you prefer
        $successMessage = "[{$errorTimestamp}] Successfully inserted tracking info into PMSDATABASEmisc";
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
    $errorLogFile = 'error_log.txt'; // Define the error log file path
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
        // Log the error
        $errorTimestamp = date('Y-m-d H:i:s'); // Format the date and time as you prefer
        $errorLogMessage = "[{$errorTimestamp}] Upsert failed: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        throw $e;
    }
}

function getFirstNonNullImportCode($originDBConnection, $tableName)
{
    $errorLogFile = 'error_log.txt'; // Define the error log file path
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
        $errorTimestamp = date('Y-m-d H:i:s'); // Format the date and time as you prefer
        $errorLogMessage = "[{$errorTimestamp}] getFirstNonNullImportCode failed: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
    }

    return $importCode;
}



////Function to create and populate the ReservationlibProperty Table Associative Array
function createReservationLibProperty($reservations) {
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    try {
        $reservationLibProperty = [];
        $seenCodes = [];

        foreach ($reservations as $reservation) {
            // Validate if necessary fields exist in $reservation
            if (!isset($reservation['extracted_property_code']) || !isset($reservation['extracted_chain_code'])) {
                throw new Exception("Necessary fields missing in the reservation data.");
            }

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
    } catch (Exception $e) {
        // Handle the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] Error in createReservationLibProperty: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if you need further handling outside this function
        throw $e;
    }
}


////Function to create and populate the ReservationlibSource Table Associative Array
function createReservationLibSource($data) {
    $errorLogFile = 'error_log.txt'; // Define the error log file path

        if (empty($data)) {
            throw new Exception("Invalid data array.");
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
                        // Additional fields can be added here
                    ];

                    // Check for duplicate rows
                    if (!in_array($reservationLibSource, $result)) {
                        $result[] = $reservationLibSource;
                    }
                }
            }
        }

        return $result;


}


////Function to create and populate the ReservationlibRoomType Table Associative Array
function createReservationLibRoomType($data) {
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    try {
        if (empty($data)) {
            throw new Exception("Invalid data array.");
        }

        $result = [];

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
        // Handle the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] Error in createReservationLibRoomType: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if you need further handling outside this function
        throw $e;
    }
}

////Function to create and populate the ReservationLibStayStatus Table Associative Array
function createReservationLibStayStatus($data) {
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    try {
        if (empty($data)) {
            throw new Exception("Invalid data array.");
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
                // Additional fields can be added here
            ];

            // Check for duplicate rows
            if (!in_array($reservationLibStayStatus, $result)) {
                $result[] = $reservationLibStayStatus;
            }
        }

        return $result;
    } catch (Exception $e) {
        // Handle the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] Error in createReservationLibStayStatus: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if you need further handling outside this function
        throw $e;
    }
}



////Function to create and populate the ReservationGroup Table Associative Array
function createReservationGroup($data) {
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    try {
        if (empty($data)) {
            throw new Exception("Invalid data array.");
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
                // Additional fields can be added here
            ];

            // Check for duplicate rows
            if (!in_array($reservationGroup, $result)) {
                $result[] = $reservationGroup;
            }
        }

        return $result;
    } catch (Exception $e) {
        // Handle the exception
        $errorTimestamp = date('Y-m-d H:i:s');
        $errorLogMessage = "[{$errorTimestamp}] Error in createReservationGroup: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if needed
        throw $e;
    }
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
    $serviceItems = [];
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
                'orderType' => 'OTHERS',
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
function createArrRESERVATIONstay(
    $connection,
    $myDataSemiParsed,
    $arrRESERVATIONlibSource,
    $arrRESERVATIONlibProperty
) {
    $arrRESERVATIONStay = [];

    // Create lookup arrays for source and property Ids
    $sourceLookup = createLookup($connection, 'RESERVATIONlibSource', 'sourceName', 'sourceType');
    $propertyLookup = createLookup($connection, 'RESERVATIONlibProperty', 'propertyCode', 'chainCode');

    foreach ($myDataSemiParsed as $entry) {
        $profiles = json_decode($entry['profiles'], true) ?? [];
        $profileData = $profiles[0] ?? []; // Assuming the first profile is relevant
        $sourceName = $profileData['names'][0]['name'] ?? 'UNKNOWN';
        $sourceType = $profileData['type'] ?? 'UNKNOWN';
        $propertyCode = $entry['extracted_property_code'] ?? 'UNKNOWN';
        $chainCode = $entry['extracted_chain_code'] ?? 'UNKNOWN';

        // Convert createdDateTime and lastModifiedDateTime to Unix timestamps
        $createDateTime = isset($entry['createdDateTime']) ? $entry['createdDateTime'] : null;
        $modifyDateTime = isset($entry['lastModifiedDateTime']) ? $entry['lastModifiedDateTime'] : null;

        // Populate libSourceId and libPropertyId based on lookup
        $libSourceId = $sourceLookup[$sourceName][$sourceType] ?? null;
        $libPropertyId = $propertyLookup[$propertyCode][$chainCode] ?? null;

        $arrRESERVATIONStay[] = [
            'createDateTime' => strtotime($createDateTime),
            'modifyDateTime' => strtotime($modifyDateTime),
            'startDate' => $entry['arrival'] ?? null,
            'endDate' => $entry['departure'] ?? null,
            'createdBy' => $entry['createdBy'] ?? null, // Extract createdBy field
            'metaData' => null,
            'extPMSConfNum' => $entry['confirmation_number'] ?? null,
            'extGuestID' => $entry['extracted_guest_id'],
            'dataSource' => 'HAPI', // Assuming 'HAPI' is constant
            'libSourceId' => $libSourceId,
            'libPropertyId' => $libPropertyId,
            'propertyCode' => $propertyCode,
            'chainCode' => $chainCode,
            'sourceName' => $sourceName,
            'sourceType' => $sourceType,
        ];
    }

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

function indexArrReservationStay($arrRESERVATIONstay) {
    $indexedStays = [];
    foreach ($arrRESERVATIONstay as $stay) {
        // Create a unique key for each stay
        $key = $stay['startDate'] . '|' . $stay['endDate'] . '|' . strtotime($stay['createDateTime']) . '|' . strtotime($stay['modifyDateTime']);
        $indexedStays[$key] = $stay['id'];
    }
    return $indexedStays;
}

function createArrRESERVATIONstayStatusStay($myDataSemiParsed, $arrRESERVATIONstay, $arrRESERVATIONlibStayStatus) {
    $arrRESERVATIONstayStatusStay = [];

    // Indexing arrRESERVATIONstay for faster lookup
    $indexArrReservationStay = [];
    foreach ($arrRESERVATIONstay as $stay) {
        // Use string values for createDateTime and modifyDateTime for consistent comparison
        $indexKey = $stay['createDateTime'] . '_' . $stay['modifyDateTime'] . '_' . $stay['startDate'] . '_' . $stay['endDate'];
        $indexArrReservationStay[$indexKey] = $stay['id'];
    }

    foreach ($myDataSemiParsed as $entry) {
        // Convert the entry dates to Unix timestamps for matching
        $createTimestamp = isset($entry['createdDateTime']) ? (string)strtotime($entry['createdDateTime']) : '0';
        $modifyTimestamp = isset($entry['lastModifiedDateTime']) ? (string)strtotime($entry['lastModifiedDateTime']) : '0';

        // Find the stayStatusID based on statusName
        $stayStatusID = null;
        foreach ($arrRESERVATIONlibStayStatus as $status) {
            if ($status['statusName'] === ($entry['ext_status'] ?? 'UNKNOWN')) {
                $stayStatusID = $status['id'];
                break;
            }
        }

        // Construct the index key for lookup
        $lookupKey = $createTimestamp . '_' . $modifyTimestamp . '_' . $entry['arrival'] . '_' . $entry['departure'];

        // Look up the stayID using the index
        $stayID = $indexArrReservationStay[$lookupKey] ?? null;

        // Create the array for this item
        $arrRESERVATIONstayStatusStay[] = [
            'cancelledBy' => $entry['cancellationDetails']['cancelledBy'] ?? null,
            'cancellationDateTime' => $entry['cancellationDetails']['cancellationDateTime'] ?? null,
            'cancellationReasonCode' => $entry['cancellationDetails']['cancellationReasonCode'] ?? null,
            'cancellationReasonText' => $entry['Cancellation']['cancellationReasonText'] ?? null,
            'dataSource' => 'HAPI', // Assuming 'HAPI' is constant
            'stayID' => $stayID,
            'createDateTime' => $createTimestamp,
            'modifyDateTime' => $modifyTimestamp,
            'startDate' => $entry['arrival'] ?? null,
            'endDate' => $entry['departure'] ?? null,
            'extGuestID' => $entry['extracted_guest_id'],
            'stayStatusID' => $stayStatusID,
            'statusName' => $entry['ext_status'] ?? 'UNKNOWN'
        ];
    }

    return $arrRESERVATIONstayStatusStay;
}


function createArrReservationRoomDetails($myDataSemiParsed, $arrCUSTOMERcontact, $arrRESERVATIONstay, $arrRESERVATIONlibRoomType, $arrRESERVATIONlibRoomClass, $arrRESERVATIONlibRoom) {
    // Create indexed arrays for fast lookup
    $indexedContacts = [];
    foreach ($arrCUSTOMERcontact as $contact) {
        $index = $contact['firstName'] . '|' . $contact['lastName'] . '|' . $contact['extGuestID'];
        $indexedContacts[$index] = $contact['id'];
    }

    $indexedStays = [];
    foreach ($arrRESERVATIONstay as $stay) {
        $index = $stay['createDateTime'] . '|' . $stay['modifyDateTime'] . '|' . $stay['startDate'] . '|' . $stay['endDate'];
        $indexedStays[$index] = $stay['id'];
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

    $arrRESERVATIONroomDetails = [];
    foreach ($myDataSemiParsed as $entry) {
        // Existing logic to decode and extract guest details...

        // New logic to decode and extract prices and ratePlans
        $pricesData = json_decode($entry['prices'], true);
        $ratePlansData = json_decode($entry['ratePlans'], true);
        $amount = (!empty($pricesData) && isset($pricesData[0]['amount'])) ? $pricesData[0]['amount'] : null;
        $ratePlanCode = (!empty($ratePlansData) && isset($ratePlansData[0]['code'])) ? $ratePlansData[0]['code'] : null;

        // Decode and extract guest details
        $guestData = json_decode($entry['guests'], true);
        $guestDetails = $guestData[0]['guest'] ?? null;

        // Extract room number and typeCode from occupiedUnits
        $occupiedUnits = json_decode($entry['occupiedUnits'], true);
        $roomNumber = $occupiedUnits[0]['unitId'] ?? 'UNKNOWN';
        $typeCode = $occupiedUnits[0]['unitTypeCode'] ?? 'UNKNOWN';

        // Lookup libRoomId and libRoomTypeId based on roomNumber and typeCode
        $libRoomId = $indexedLibRooms[$roomNumber] ?? null;
        $libRoomTypeId = $indexedRoomTypes[$typeCode] ?? $defaultLibRoomTypeId;


        if ($guestDetails) {
            $givenName = $guestDetails['names'][0]['givenName'] ?? null;
            $surname = $guestDetails['names'][0]['surname'] ?? null;
            $contactIndex = $givenName . '|' . $surname . '|' . $entry['extracted_guest_id'];
            $contactID = $indexedContacts[$contactIndex] ?? null;

            $createDateTime = strtotime($entry['createdDateTime']);
            $modifyDateTime = strtotime($entry['lastModifiedDateTime']);
            $stayIndex = $createDateTime . '|' . $modifyDateTime . '|' . $entry['arrival'] . '|' . $entry['departure'];
            $stayId = $indexedStays[$stayIndex] ?? null;

            // Room type and class lookups
//            $roomTypeCode = json_decode($entry['occupiedUnits'], true)[0]['unitTypeCode'] ?? null;
//            $libRoomTypeId = $indexedRoomTypes[$roomTypeCode] ?? null;
            $className = 'UNKNOWN'; // Replace with actual logic to determine class name
            $libRoomClassId = $indexedRoomClasses[$className] ?? null;

            // Continue building the details array
            $arrRESERVATIONroomDetails[] = [
                'startDate' => $entry['arrival'] ?? null,
                'endDate' => $entry['departure'] ?? null,
                'amount' => $amount,
                'ratePlanCode' => $ratePlanCode,
                'isBlocked' => isset($entry['Blocks']) && !$entry['Blocks']['isempty'] ? 1 : 0,
                'isComplimentary' => intval($entry['isComplimentary']) ?? null,
                'isHouseUse' => $entry['isHouseUse'] ?? 0,
                'contactID' => $contactID,
                'firstName' => $givenName,
                'lastName' => $surname,
                'extGuestId' => $entry['extracted_guest_id'] ?? null,
                'stayId' => $stayId,
                'createDateTime' => ($timestamp = strtotime($entry['createdDateTime'])) ? strval($timestamp) : null,
                'modifyDateTime' => ($timestamp = strtotime($entry['lastModifiedDateTime'])) ? strval($timestamp) : null,
                'extPMSConfNum' => $entry['confirmation_number'] ?? null,
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
}



function createArrSERVICESfolioOrders($normalizedData, $arrCUSTOMERcontact, $arrRESERVATIONstay, $arrSERVICESpayment, $arrSERVICESlibServiceItems, $arrSERVICESlibFolioOrdersType) {
    // Indexing customer contacts for fast lookup
    $indexedCustomerContacts = [];
    foreach ($arrCUSTOMERcontact as $contact) {
        $index = $contact['firstName'] . '|' . $contact['lastName'] . '|' . $contact['extGuestID'];
        $indexedCustomerContacts[$index] = $contact['id'];
    }

    // Indexing reservation stays for fast lookup
    $indexedReservationStays = [];
    foreach ($arrRESERVATIONstay as $stay) {
        $index = $stay['createDateTime'] . '|' . $stay['modifyDateTime'] . '|' . $stay['startDate'] . '|' . $stay['endDate'];
        $indexedReservationStays[$index] = $stay['id'];
    }

    // Indexing payments for fast lookup
    $indexedPayments = [];
    foreach ($arrSERVICESpayment as $payment) {
        $index = $payment['paymentAmount'] . '|' . $payment['currencyCode'];
        $indexedPayments[$index] = $payment['id'];
    }


// Indexing libServiceItems for fast lookup
    $indexedLibServiceItems = [];
    foreach ($arrSERVICESlibServiceItems as $item) {
        $index = $item['itemCode'] . '|' . $item['ratePlanCode'];
        $indexedLibServiceItems[$index] = $item['id'];
    }

    // Indexing libFolioOrdersType for fast lookup, adjusted for nested structure
    $indexedLibFolioOrdersType = [];
    foreach ($arrSERVICESlibFolioOrdersType as $entry) {
        $folioOrderTypeData = $entry['SERVICESlibFolioOrdersType'];
        $indexedLibFolioOrdersType[$folioOrderTypeData['orderType']] = $folioOrderTypeData['id'];
    }

    $arrSERVICESfolioOrders = [];

    foreach ($normalizedData as $data) {
        $guestDetails = $data['guests'][0]['guest'] ?? null;
        $firstName = $guestDetails['names'][0]['givenName'] ?? 'UNKNOWN';
        $lastName = $guestDetails['names'][0]['surname'] ?? 'UNKNOWN';
        $extGuestId = $data['extracted_guest_id'] ?? null;
        $createDateTime = strtotime($data['createdDateTime']) ?? null;
        $modifyDateTime = strtotime($data['lastModifiedDateTime']) ?? null;
        $startDate = $data['arrival'] ?? null; // Assuming these are already in the correct format
        $endDate = $data['departure'] ?? null;
        $stayId = $indexedCustomerContacts[$extGuestId] ?? null; // Lookup for stayId
        // Populate libServiceItemsId using itemCode and ratePlanCode
        $itemCode = $data['services'][0]['code'] ?? 'UNKNOWN';
        $ratePlanCode = $data['prices'][0]['ratePlanCode'] ?? 'UNKNOWN';
        $libServiceItemsIndex = $itemCode . '|' . $ratePlanCode;
        $libServiceItemsId = $indexedLibServiceItems[$libServiceItemsIndex] ?? $indexedLibServiceItems['UNKNOWN' . '|' . 'UNKNOWN'];
        // Retrieve the libFolioOrdersTypeId using the folioOrderType
        // Determine the folioOrderType here
        $folioOrderType = null;


        // Now use the determined folioOrderType for the lookup
        $libFolioOrdersTypeId = isset($indexedLibFolioOrdersType[$folioOrderType]) ? $indexedLibFolioOrdersType[$folioOrderType] : null;


        // Create index for the contact id lookup
        $contactIndex = $firstName . '|' . $lastName . '|' . $extGuestId;
        // Lookup for stayId using the index
        $customerId = $indexedCustomerContacts[$contactIndex] ?? null;


        // Create index for stay lookup
        $stayIndex = $createDateTime . '|' . $modifyDateTime . '|' . $startDate . '|' . $endDate;
        // Lookup for stayId using the index
        $stayId = $indexedReservationStays[$stayIndex] ?? null;

        // Populate paymentId using paymentAmount and currencyCode
        $paymentAmount = null;
        $currencyCode = $data['currency']['code'] ?? null;
        $paymentIndex = $paymentAmount . '|' . $currencyCode;
        $paymentId = $indexedPayments[$paymentIndex] ?? null;




        // Common fields for all folio orders
        $commonFields = [
            'dataSource' => 'HAPI',
            'contactId' => $customerId,
            'firstName' => $firstName,
            'lastName' => $lastName,
            'extGuestId' => $extGuestId,
            'stayId' => $stayId,
            'startDateLookup' => $startDate,
            'endDateLookup' => $endDate,
            'extPMSConfNum' => $data['confirmation_number'] ?? null,
            'createDateTime' => $createDateTime ?? null,
            'modifyDateTime' => $modifyDateTime ?? null,
            'paymentId' => $paymentId,
            'paymentAmount' =>  $paymentAmount,
            'currencyCode' => $currencyCode,
            'libServiceItemsId' => $libServiceItemsId,
            'itemCode' => $data['services'][0]['code'] ?? 'UNKNOWN',
            'ratePlanCode' => $data['prices'][0]['ratePlanCode'] ?? 'UNKNOWN',
            'libFolioOrdersTypeId' => $libFolioOrdersTypeId,

        ];



        // SERVICE type folio order
        if (isset($data['services'])) {
            foreach ($data['services'] as $service) {
                $serviceOrder = $commonFields;
                $serviceOrder['folioOrderType'] = 'SERVICE';
                $serviceOrder['unitCount'] = $service['quantity'] ?? null;
                $serviceOrder['unitPrice'] = null; // Calculate if needed
                $serviceOrder['fixedCost'] = null; // Calculate if needed
                $serviceOrder['amountBeforeTax'] = null; // Calculate if needed
                $serviceOrder['amountAfterTax'] = null; // Calculate if needed
                $serviceOrder['postingFrequency'] = null;
                $serviceOrder['startDate'] = null;
                $serviceOrder['endDate']  = null;
                $serviceOrder['amount']  = null;
                $serviceOrder['fixedChargesQuantity']  = null;
                $serviceOrder['transferId']  =  null;
                $serviceOrder['transferDateTime']  =  null;
                $serviceOrder['transferOnArrival']  =  null;
                $serviceOrder['isIncluded']  =  $data['services']['isIncluded'] ?? null;
                // Additional SERVICE specific fields go here...

                $arrSERVICESfolioOrders[] = $serviceOrder;
            }

        }

        // RESERVATION type folio order
        if (isset($data['bookedUnits'])) {
            $reservationOrder = $commonFields;
            $reservationOrder['folioOrderType'] = 'RESERVATION';
            $reservationOrder['unitCount'] = null; // Determine if needed
            $reservationOrder['unitPrice'] = null; // Calculate if needed
            $reservationOrder['fixedCost'] = $data['prices'][0]['amount'] ?? null;
            $reservationOrder['amountBeforeTax'] = $data['reservationTotal']['amountBeforeTax'] ?? $data['prices'][0]['amount'];
            $reservationOrder['amountAfterTax'] = $data['reservationTotal']['amountAfterTax'] ?? $data['prices'][0]['amount'] + $data['taxes'][0]['amount'];
            $reservationOrder['postingFrequency'] = null;
            $reservationOrder['startDate'] = $data['bookedUnits'][0]['start'] ?? null;
            $reservationOrder['endDate']  = $data['bookedUnits'][0]['end'] ?? null;
            $reservationOrder['amount']  = $data['prices'][0]['amount'] ?? null;
            $reservationOrder['fixedChargesQuantity']  = null;
            $reservationOrder['transferId']  =  $data['transferId']['id'] ?? null;
            $reservationOrder['transferDateTime']  =  $data['transferDateTime']['dateTime'] ?? null;
            $reservationOrder['transferOnArrival']  =  $data['transferOnArrival']['isOnArrival'] ?? null;
            $reservationOrder['isIncluded']  =  null;
            // Additional RESERVATION specific fields go here...

            $arrSERVICESfolioOrders[] = $reservationOrder;
        }

        // OTHER type folio order
        if (isset($data['fixedCharges'])) {
            foreach ($data['fixedCharges'] as $fixedCharge) {
                $otherOrder = $commonFields;
                $otherOrder['folioOrderType'] = 'OTHERS';
                $otherOrder['unitCount'] = $fixedCharge['quantity'] ?? null;
                $otherOrder['unitPrice'] = $fixedCharge['amount'] ?? null;
                $otherOrder['fixedCost'] = ($otherOrder['unitCount'] ?? 0) * ($otherOrder['unitPrice'] ?? 0);
                // Additional OTHER specific fields go here...
                $otherOrder['amountBeforeTax'] = $data['prices'][0]['amount'] ?? null;
                $otherOrder['amountAfterTax'] = $data['prices'][0]['amount'] + $data['taxes'][0]['amount'] ?? null;
                $otherOrder['postingFrequency'] = $data['fixedCharges']['postingFrequency'] ?? null;
                $otherOrder['startDate'] = $data['fixedCharges']['start'] ?? null;
                $otherOrder['endDate']  = $data['fixedCharges']['end'] ?? null;
                $otherOrder['amount']  = null;
                $otherOrder['fixedChargesQuantity']  = null;
                $otherOrder['transferId']  =  null;
                $otherOrder['transferDateTime']  =  null;
                $otherOrder['transferOnArrival']  =  null;
                $otherOrder['isIncluded']  =  null;

                $arrSERVICESfolioOrders[] = $otherOrder;
            }

        }
        // Add UNKNOWN type folio order if none of the above apply
        if (empty($arrSERVICESfolioOrders)) {
            $unknownOrder = $commonFields;
//            $unknownOrder['folioOrderType'] = 'UNKNOWN';
            // Add default/unknown values for all other fields
            $arrSERVICESfolioOrders[] = $unknownOrder;
        }

    }
    populateLibFolioOrdersTypeId($arrSERVICESfolioOrders, $arrSERVICESlibFolioOrdersType);



    return $arrSERVICESfolioOrders;
}


function populateLibFolioOrdersTypeId(&$arrSERVICESfolioOrders, $arrSERVICESlibFolioOrdersType) {
    // Indexing libFolioOrdersType for fast lookup
    $indexedLibFolioOrdersType = [];
    foreach ($arrSERVICESlibFolioOrdersType as $entry) {
        $orderType = $entry['orderType'];
        $orderId = $entry['id']; // Directly accessing the 'id'
        $indexedLibFolioOrdersType[$orderType] = $orderId;
    }

    // Loop through each folio order and assign the correct libFolioOrdersTypeId
    foreach ($arrSERVICESfolioOrders as $key => &$order) {
        if (isset($order['folioOrderType']) && isset($indexedLibFolioOrdersType[$order['folioOrderType']])) {
            $order['libFolioOrdersTypeId'] = $indexedLibFolioOrdersType[$order['folioOrderType']];
        } else {
            $order['libFolioOrdersTypeId'] = null; // Set to null if no match is found
        }
    }
}


function removeDuplicateOrders($arrSERVICESfolioOrders) {
    // Serialize each order to make it a string
    $serializedOrders = array_map('serialize', $arrSERVICESfolioOrders);

    // Remove duplicates
    $uniqueSerializedOrders = array_unique($serializedOrders);

    // Unserialize each order to convert it back to its original array form
    $uniqueOrders = array_map('unserialize', $uniqueSerializedOrders);

    return $uniqueOrders;
}





function normalizeMyDataSemiParsed($myDataSemiParsed) {
    $normalizedData = [];

    foreach ($myDataSemiParsed as $entryKey => $entryValue) {
        // If the value is a JSON string, decode it
        if (is_string($entryValue) && isJson($entryValue)) {
            $decodedJson = json_decode($entryValue, true);
            if (json_last_error() === JSON_ERROR_NONE) {
                // If decoding was successful, merge the decoded JSON into the normalized data
                $normalizedData[$entryKey] = $decodedJson;
            } else {
                // Keep the original value if JSON decoding fails
                $normalizedData[$entryKey] = $entryValue;
            }
        } elseif (is_array($entryValue)) {
            // Recursively normalize nested arrays
            $normalizedData[$entryKey] = normalizeMyDataSemiParsed($entryValue);
        } else {
            // Copy over the value directly if it's not a JSON string or an array
            $normalizedData[$entryKey] = $entryValue;
        }
    }



    return $normalizedData;
}

function isJson($string) {
    json_decode($string);
    return json_last_error() === JSON_ERROR_NONE;
}
?>