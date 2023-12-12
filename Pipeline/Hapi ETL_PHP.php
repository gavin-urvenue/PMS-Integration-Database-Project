<?php


// Simulate fetching data from source MySQL database

// Function to fetch data from a MySQL table and store it in a PHP associative  array



function fetchDataFromMySQLTable($tableName)
{
    // Simulate a database connection
    $connection = mysqli_connect('localhost:3306', 'urvenue', 'Password1!', 'Testing');

    // Check connection
    if (!$connection) {
        $errorMessage = 'Connection failed: ' . mysqli_connect_error();
        error_log($errorMessage, 3, 'error_log.txt'); // Log connection error
        die($errorMessage);
    }

    // Simulate a database query to fetch data
    $query = "SELECT * FROM $tableName";
    $result = mysqli_query($connection, $query);

    // Check for query success
    if (!$result) {
        $errorMessage = 'Query failed: ' . mysqli_error($connection);
        error_log($errorMessage, 3, 'error_log.txt'); // Log query error
        die($errorMessage);
    }

    // Fetch data and store it in an array
    $data = [];
    while ($row = mysqli_fetch_assoc($result)) {
        $data[] = $row;
    }

    // Log the number of records fetched
    $recordCount = count($data);
    $successMessage = "Successfully fetched $recordCount records from $tableName";
    error_log($successMessage, 3, 'error_log.txt');

    // Close the database connection
    mysqli_close($connection);

    return $data;
}
//function fetchDataAndUpdateSchemaVersion($tableName, $host, $username, $password, $database, $secondDbConfig)
//{
//    $connection = mysqli_connect($host, $username, $password, $database);
//
//    if (!$connection) {
//        $errorMessage = 'Connection failed: ' . mysqli_connect_error();
//        error_log($errorMessage, 3, 'error_log.txt');
//        return ['error' => $errorMessage];
//    }
//
//    $query = "SELECT * FROM $tableName";
//    $result = mysqli_query($connection, $query);
//
//    if (!$result) {
//        $errorMessage = 'Query failed: ' . mysqli_error($connection);
//        error_log($errorMessage, 3, 'error_log.txt');
//        mysqli_close($connection);
//        return ['error' => $errorMessage];
//    }
//
//    $data = [];
//    while ($row = mysqli_fetch_assoc($result)) {
//        $data[] = $row;
//    }
//
//    $recordCount = count($data);
//    $successMessage = "Successfully fetched $recordCount records from $tableName";
//    error_log($successMessage, 3, 'error_log.txt');
//
//    // Connect to the second database
//    $secondConnection = mysqli_connect($secondDbConfig['host'], $secondDbConfig['username'], $secondDbConfig['password'], $secondDbConfig['database']);
//
//    if (!$secondConnection) {
//        $errorMessage = 'Second database connection failed: ' . mysqli_connect_error();
//        error_log($errorMessage, 3, 'error_log.txt');
//        mysqli_close($connection);
//        return ['error' => $errorMessage];
//    }
//
//    // Retrieve the highest etlTSTAMP value from the second database
//    $secondQuery = "SELECT MAX(etlTSTAMP) AS max_etlTSTAMP FROM PMSDATABASEmisc";
//    $secondResult = mysqli_query($secondConnection, $secondQuery);
//
//    if (!$secondResult) {
//        $errorMessage = 'Query failed: ' . mysqli_error($secondConnection);
//        error_log($errorMessage, 3, 'error_log.txt');
//        mysqli_close($connection);
//        mysqli_close($secondConnection);
//        return ['error' => $errorMessage];
//    }
//
//    $maxEtlTSTAMP = mysqli_fetch_assoc($secondResult)['max_etlTSTAMP'];
//
//    // Insert a new entry into the second database's PMSDATABASEmisc table
//    if ($maxEtlTSTAMP === null) {
//        $maxEtlTSTAMP = time();
//    }
//
//    $secondQuery = "INSERT INTO PMSDATABASEmisc (schemaVersion, etlTSTAMP) VALUES ('1.0.0', $maxEtlTSTAMP)";
//    $secondResult = mysqli_query($secondConnection, $secondQuery);
//
//    if (!$secondResult) {
//        $errorMessage = 'Insert query failed: ' . mysqli_error($secondConnection);
//        error_log($errorMessage, 3, 'error_log.txt');
//        mysqli_close($connection);
//        mysqli_close($secondConnection);
//        return ['error' => $errorMessage];
//    }
//
//    // Close database connections
//    mysqli_close($connection);
//    mysqli_close($secondConnection);
//
//    return ['success' => true, 'maxEtlTSTAMP' => $maxEtlTSTAMP];
//}
//
//// Example usage
//$firstDbConfig = [
//    'host' => 'first_db_host',
//    'username' => 'first_db_username',
//    'password' => 'first_db_password',
//    'database' => 'first_db_name',
//];
//
//$secondDbConfig = [
//    'host' => 'second_db_host',
//    'username' => 'second_db_username',
//    'password' => 'second_db_password',
//    'database' => 'second_db_name',
//];
//
//$tableName = 'your_table_name'; // Replace with your table name
//$result = fetchDataAndUpdateSchemaVersion($tableName, $firstDbConfig['host'], $firstDbConfig['username'], $firstDbConfig['password'], $firstDbConfig['database'], $secondDbConfig);
//
//if (isset($result['error'])) {
//    // Handle the error
//    echo 'Error: ' . $result['error'];
//} else {
//    // Process the success
//    echo 'Success! Max etlTSTAMP: ' . $result['maxEtlTSTAMP'];
//}

// Example usage
//$firstDbConfig = [
//    'host' => 'first_db_host',
//    'username' => 'first_db_username',
//    'password' => 'first_db_password',
//    'database' => 'first_db_name',
//];
//
//$secondDbConfig = [
//    'host' => 'second_db_host',
//    'username' => 'second_db_username',
//    'password' => 'second_db_password',
//    'database' => 'second_db_name',
//];
//
//$result = fetchDataAndUpdateSchemaVersionPDO($firstDbConfig, $secondDbConfig);
//
//if (isset($result['error'])) {
//    // Handle the error
//    echo 'Error: ' . $result['error'];
//} else {
//    // Process the success
//    echo 'Success! Max etlTSTAMP: ' . $result['maxEtlTSTAMP'];
//}
//





// Helper function to check if a string is a valid JSON
function isJson($string)
{
    json_decode($string);
    return (json_last_error() == JSON_ERROR_NONE);
}
//
////Function to create and populate the ReservationlibProperty Table
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

function createReservationLibRoomType($data)
{
    if (empty($data)) {
        echo "Invalid data array\n";
        return;
    }

    $result = [];
    $result[] = [
        'RESERVATIONlibroomtype' => [
            'typeName' => '',
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
                    'RESERVATIONlibroomtype' => [
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
            'groupName' => 'Unknown',
            'groupNumber' => 'Unknown',
            'groupStartDate' => '',
            'groupEndDate' => '',
            'dataSource' => 'HAPI',
        ],
    ];

    $result[] = $unknownRow;

    foreach ($data as $reservation) {
        // Extract relevant data for RESERVATIONgroup mapping
        $groupName = $reservation['groupName'] ?? 'Unknown';
        $groupNumber = $reservation['groupNumber'] ?? 'Unknown';
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

function createReservationLibRoomClass($data)
{
    if (empty($data)) {
        echo "Invalid data array\n";
        return;
    }

    $result = [];

    // Add 'Unknown' row at the beginning
    $unknownRow = [
        'RESERVATIONlibroomclass' => [
            'className' => 'Unknown',
            'dataSource' => 'HAPI',
        ],
    ];

    $result[] = $unknownRow;


    return $result;
}

function createReservationLibRoom($data)
{
    if (empty($data)) {
        echo "Invalid data array\n";
        return;
    }

    $result = [];

    // Add 'Unknown' row at the beginning
    $unknownRow = [
        'RESERVATIONlibroom' => [
            'roomNumber' => 'Unknown',
            'dataSource' => 'HAPI',
        ],
    ];

    $result[] = $unknownRow;

    foreach ($data as $reservation) {
        // Extract relevant data for RESERVATIONlibroom mapping
        $occupiedUnits = json_decode($reservation['occupiedUnits'], true);
        $unitId = $occupiedUnits[0]['unitId'] ?? 'Unknown';

        $reservationLibRoom = [
            'RESERVATIONlibroom' => [
                'roomNumber' => $unitId,
                'dataSource' => 'HAPI',
            ],
            // You can add more fields here based on your mapping
            // Example:
            // 'additionalField' => $reservation['someValue'] ?? "",
        ];

        // Check for duplicate rows
        if (!in_array($reservationLibRoom, $result)) {
            $result[] = $reservationLibRoom;
        }
    }

    // Custom sort function to move 'Unknown' to the beginning
    usort($result, function ($a, $b) {
        if ($a['RESERVATIONlibroom']['roomNumber'] === 'Unknown') {
            return -1;
        } elseif ($b['RESERVATIONlibroom']['roomNumber'] === 'Unknown') {
            return 1;
        } else {
            return strcmp($a['RESERVATIONlibroom']['roomNumber'], $b['RESERVATIONlibroom']['roomNumber']);
        }
    });

    return $result;
}

function extractGuestSurnames($guestData) {
    $surnames = [];

    foreach ($guestData as $guest) {
        $surname = $guest['guest']['names'][0]['surname'];
        $surnames[] = $surname;
    }

    return $surnames;
}

function createLoyaltyProgramArray() {
    $loyaltyProgramArray = [
        [
            'CUSTOMERlibloyaltyprogram' => [
                'Name' => 'Unknown',
                'Source' => 'Unknown',
                'metaData' => [],
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'CUSTOMERlibloyaltyprogram' => [
                'Name' => 'Fairmont',
                'Source' => 'Fairmont Banff Springs',
                'metaData' => [],
                'dataSource' => 'HAPI'
            ]
        ]
    ];

    return $loyaltyProgramArray;
}

function createContactTypeArray() {
    $contactTypeArray = [
        [
            'CUSTOMERlibcontactType' => [
                'type' => 'UNKNOWN',
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'CUSTOMERlibcontactType' => [
                'type' => 'GUEST',
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'CUSTOMERlibcontactType' => [
                'type' => 'CUSTOMER',
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'CUSTOMERlibcontactType' => [
                'type' => 'PATRON',
                'dataSource' => 'HAPI'
            ]
        ],
    ];

    return $contactTypeArray;
}
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
function extractPaymentMethodCode($data)
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

function extractServices($mainArray) {
    $servicesArray = [];

    foreach ($mainArray as $key => $entry) {
        // Check if 'services' field exists and is not null
        if (isset($entry['services']) && $entry['services'] !== null) {
            // Decode the JSON string in 'services' field
            $servicesData = json_decode($entry['services'], true);

            // Extract fields from 'serviceDetails' records
            foreach ($servicesData as &$service) {
                if (isset($service['serviceDetails']) && is_string($service['serviceDetails'])) {
                    $details = json_decode($service['serviceDetails'], true);
                    unset($service['serviceDetails']);
                    $service = array_merge($service, $details);
                }
            }

            // Move the services data to the new array, excluding 'serviceDetails'
            $servicesArray[$key] = $servicesData;

            // Remove the 'services' field from the main array
            unset($mainArray[$key]['services']);
        }
    }

    return $servicesArray;
}

function createFolioOrderTypeArray() {
    $contactTypeArray = [
        [
            'SERVICESlibFolioOrdersType' => [
                'type' => 'UNKNOWN',
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'SERVICESlibFolioOrdersType' => [
                'type' => 'RESERVATION',
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'SERVICESlibFolioOrdersType' => [
                'type' => 'SERVICE',
                'dataSource' => 'HAPI'
            ]
        ],
        [
            'SERVICESlibFolioOrdersType' => [
                'type' => 'RESORT FEE',
                'dataSource' => 'HAPI'
            ]
        ],
    ];

    return $contactTypeArray;
}

function mapCustomerContactData($array) {
    $customerContacts = [];
    $uniqueCheck = []; // Array to keep track of existing contacts to prevent duplicates

    foreach ($array as $item) {
        // Ensure 'guests' field is not null before decoding
        if (isset($item['guests']) && !is_null($item['guests'])) {
            $guests = json_decode($item['guests'], true);

            foreach ($guests as $guest) {
                $guestData = $guest['guest'];

                // Initialize the contact array with default values
                $contact = [
                    'firstName' => '',
                    'LastName' => '',
                    'title' => '',
                    'email' => '',
                    'birthDate' => '',
                    'languageCode' => '',
                    'languageFormat' => '',
                    'isPrimary' => $guestData['isPrimary'] ?? '',
                    'dataSource' => 'HAPI'
                ];

                // Check if names exist and assign them, including language details
                if (!empty($guestData['names'])) {
                    foreach ($guestData['names'] as $name) {
                        $contact['firstName'] = $name['givenName'] ?? '';
                        $contact['LastName'] = $name['surname'] ?? '';
                        // Assuming the language is part of the first name's structure
                        if (isset($name['language'])) {
                            $contact['languageCode'] = $name['language']['code'] ?? '';
                            $contact['languageFormat'] = $name['language']['format'] ?? '';
                            // Assuming we only have one name entry with a language, break out of the loop
                            break;
                        }
                    }
                }

                // Check if contact details exist and find the email
                if (!empty($guestData['contactDetails'])) {
                    foreach ($guestData['contactDetails'] as $contactDetail) {
                        if ($contactDetail['type'] === 'EMAIL') {
                            $contact['email'] = $contactDetail['value'] ?? '';
                            break;
                        }
                    }
                }

                // Create a unique identifier for each contact
                $uniqueId = $contact['firstName'] . '|' . $contact['LastName'] . '|' . $contact['email'];

                // Check if this uniqueId has already been added to prevent duplicates
                if (!isset($uniqueCheck[$uniqueId])) {
                    // Add the contact to the customerContacts array
                    $customerContacts[] = $contact;
                    // Mark this uniqueId as added
                    $uniqueCheck[$uniqueId] = true;
                }
            }
        }
    }

    return $customerContacts;
}


// Example usage:
$tableName = 'hapi_raw_reservations';
$host = 'localhost:3306';
$username = 'urvenue';
$password = 'Password1!';
$database = 'Testing';

$myData = fetchDataFromMySQLTable($tableName, $host, $username, $password, $database);

//if (isset($myData['error'])) {
//    echo 'Error: ' . $myData['error'];
//} else {
//    var_dump(mapCustomerContactData($myData));
////    var_dump($myData);
//}
// Example usage with debugging
//$reservationData = $myData;
//$customerContactData = mapCustomerContactData($reservationData);
//error_log(print_r($myData, true));



// Declare Variables:
$tableName = 'hapi_raw_reservations';
$myData = fetchDataFromMySQLTable($tableName);




// Example usage

$myDataSemiParsed = $myData;  // Use $inputArray instead of $myData

//// Output the result
var_dump(mapCustomerContactData($myDataSemiParsed));





?>
