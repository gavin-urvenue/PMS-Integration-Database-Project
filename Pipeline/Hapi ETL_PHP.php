<?php


// Simulate fetching data from source MySQL database

// Function to fetch data from a MySQL table and store it in a PHP data structure
function fetchDataFromMySQLTable($tableName)
{
    // Simulate a database connection
    $connection = mysqli_connect('10.10.1.51:3306', 'urvenue', 'Password1!', 'Testing');

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
//    error_log($successMessage, 3, 'error_log.txt');

    // Close the database connection
    mysqli_close($connection);

    return $data;
}

function separateJSONFields($data)
{
    $separatedData = [];

    foreach ($data as $key => $value) {
        if (is_array($value)) {
            // Recursively call the function to handle nested arrays
            $nestedData = separateJSONFields($value);

            // Merge the nested data with the result
            $separatedData[$key] = $nestedData;
        } else {
            // Check if the value is a JSON string
            if (is_string($value) && isJson($value)) {
                // Decode the JSON string
                $decodedValue = json_decode($value, true);

                // Check if decoding was successful and if it's an array
                if (json_last_error() === JSON_ERROR_NONE && is_array($decodedValue)) {
                    // If it's an array, recursively call the function to handle nested JSON
                    $nestedData = separateJSONFields($decodedValue);

                    // Merge the nested data with the result
                    $separatedData[$key] = $nestedData;
                } else {
                    // If it's not an array, create a new key in the result
                    $separatedData[$key] = $decodedValue;
                }
            } else {
                // If not an array or JSON string, keep the original value
                $separatedData[$key] = $value;
            }
        }
    }

    return $separatedData;
}

// Helper function to check if a string is a valid JSON
function isJson($string) {
    json_decode($string);
    return (json_last_error() == JSON_ERROR_NONE);
}


// Example usage:
$tableName = 'hapi_raw_reservations';
$myData = fetchDataFromMySQLTable($tableName);

// Example usage
$originalArray = $myData;
$separatedArray = separateJSONFields($originalArray);

// Initialize an empty array to store systemIds
$systemIds = [];

// Iterate through the main array
foreach ($separatedArray as $item) {
    // Check if 'referenceIds' key exists in the current item
    if (isset($item['referenceIds']) && is_array($item['referenceIds'])) {
        // Iterate through the 'referenceIds' array
        foreach ($item['referenceIds'] as $reference) {
            // Check if 'systemId' key exists in the current reference
            if (isset($reference['systemId'])) {
                // Add the 'systemId' value to the $systemIds array
                $systemIds[] = $reference['systemId'];
            }
        }
    }
}

// $systemIds now contains all the 'systemId' values from the original array
print_r($systemIds);
?>
//$result = in_array('ECARPENTER', $separatedArray);
//
//var_dump($result); // bool(true)
//// Display the fetched data
//print_r($myData);

// // Simulate saving data to destination MySQL database
// function saveDataToDestination($tableName, $data)
// {
//     // Simulate a database connection
//     $connection = mysqli_connect('destination-host', 'destination-username', 'destination-password', 'destination-database');
//
//     // Check connection
//     if (!$connection) {
//         die('Connection failed: ' . mysqli_connect_error());
//     }
//
//     // Simulate starting a transaction
//     mysqli_begin_transaction($connection);
//
//     try {
//         // Simulate inserting data into the destination table
//         $columns = implode(', ', array_keys($data));
//         $values = implode(', ', array_map(function ($value) {
//             return "'" . mysqli_real_escape_string($GLOBALS['connection'], $value) . "'";
//         }, $data));
//
//         $query = "INSERT INTO $tableName ($columns) VALUES ($values)";
//         mysqli_query($GLOBALS['connection'], $query);
//
//         // Simulate committing the transaction
//         mysqli_commit($GLOBALS['connection']);
//     } catch (\Exception $e) {
//         // Simulate rolling back the transaction on exception
//         mysqli_rollback($GLOBALS['connection']);
//
//         // Log the error
//         error_log('An error occurred during ETL process: ' . $e->getMessage());
//
//         // Rethrow the exception
//         throw $e;
//     } finally {
//         // Simulate closing the database connection
//         mysqli_close($GLOBALS['connection']);
//     }
// }

// // ETL process in base PHP (PHP 7.4.33)
// try {
//     // Simulate starting a transaction
//     mysqli_begin_transaction($connection);

//     // Fetch data from source MySQL database
//     $sourceData = fetchDataFromSource();

//     // Transform: Perform data transformations
//     $transformedData = array_map(function ($item) {
//         $additionalData = json_decode($item['additional_data'], true);

//         // Clean data (example: remove non-alphanumeric characters from the title)
//         $cleanedTitle = preg_replace('/[^a-zA-Z0-9 ]/', '', $item['title']);

//         return [
//             'id' => $item['id'],
//             'cleaned_title' => $cleanedTitle,
//             'additional_info' => $additionalData['info'] ?? null,
//         ];
//     }, $sourceData);

//     // Load: Save transformed data to destination MySQL database (two tables)
//     foreach ($transformedData as $row) {
//         saveDataToDestination('table1', ['id' => $row['id'], 'cleaned_title' => $row['cleaned_title']]);
//         saveDataToDestination('table2', ['id' => $row['id'], 'additional_info' => $row['additional_info']]);
//     }

//     // Simulate committing the transaction
//     mysqli_commit($connection);

//     // Log success
//     error_log('ETL process completed successfully.');
//     echo 'ETL process completed successfully.' . PHP_EOL;
// } catch (\Exception $e) {
//     // Simulate rolling back the transaction on exception
//     mysqli_rollback($connection);

//     // Log the error
//     error_log('An error occurred during ETL process: ' . $e->getMessage());
//     echo 'An error occurred during ETL process: ' . $e->getMessage() . PHP_EOL;
// }
