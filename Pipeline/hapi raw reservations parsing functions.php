<?php

ini_set('memory_limit', '8192M'); // Increase to 256MB, or a value that suits your needs


$sshTunnelHost = '127.0.0.1'; // Localhost because the port is forwarded locally
$sshTunnelPort = '3307'; // The local port you've forwarded
$mysqlUser = 'gavin';
$mysqlPassword = 'YohMq95r@!';
$mysqlDatabase = 'hapi_raw';

// Connects to the local end of the SSH tunnel
$originDBConnection = new mysqli($sshTunnelHost, $mysqlUser, $mysqlPassword, $mysqlDatabase, $sshTunnelPort);

// Check for successful connection
if ($originDBConnection->connect_error) {
    die('Connect Error (' . $originDBConnection->connect_errno . ') ' . $originDBConnection->connect_error);
}


//$connection = ssh2_connect('34.68.48.31', 22022);
//ssh2_auth_pubkey_file(
//    $connection,
//    'gavin',
//    '/Users/gavin/.ssh/urcrons_key',
//);


function isJson($string) {
    json_decode($string);
    return json_last_error() === JSON_ERROR_NONE;
}
function fetchDataFromMySQLTable($tableName, $originDBConnection) {
    $errorLogFile = 'error_log.txt'; // Define the error log file path

    try {
        // Check connection
        if ($originDBConnection->connect_error) {
            throw new Exception('Connection failed: ' . $originDBConnection->connect_error);
        }

        // Start transaction
        $originDBConnection->begin_transaction();



        // Prepare and execute query
        $query = "SELECT * FROM $tableName where year(arrival) = 2024 and month(arrival) = 1";
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
        $errorTimestamp = date('Y-m-d H:i:s'); // Format the date and time as you prefer
        $errorLogMessage = "[{$errorTimestamp}] fetchDataFromMySQLTable failed: " . $e->getMessage() . PHP_EOL;
        error_log($errorLogMessage, 3, $errorLogFile);
        // Optionally rethrow the exception if you want to handle it further up the call stack
        throw $e;
    }
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


$myDataSemiParsed2 = fetchDataFromMySQLTable('hapi_raw_reservations', $originDBConnection);

print_r(normalizeMyDataSemiParsed($myDataSemiParsed2));

?>