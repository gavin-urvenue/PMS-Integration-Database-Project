<?php
//This script is designed to connect to the hapi_raw_reservations table which holds the raw api data from Fairmont's
//Opera HAPI system (origin) and the pms_db oltp database (destination). It's designed to take the hapi_raw_reservations data,
//organize it into associative arrays based on the end database tables, and upsert the data into the tables.
//It then pulls the data from these tables into associative arrays in preparation for upserts into the next tables.
// This is due to the primary key field of each table, id, being required for the foreign keys of other tables in the database.
// The id field is generated at the database level via triggers.
// Thus, the flow is hapi_raw_reservations -> associative array -> table -> associative array.
//This happens in 3 main phases. First an insert of data into the parent tables, then an insert into the child table,
//and then an insert into grandchild tables, with the child tables being dependent on the parent tables and the
//grandchild tables being dependent on the child tables.

//Setting up Error Logging
error_reporting(E_ALL);
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);


//libraries and helpers
require_once 'config.php';
include 'upsert_functions.php';
include 'array_functions.php';
//variables
$schemaVersion = SCHEMA_VERSION;
// Database Connections:
$originTableName = ORIGIN_TABLE_NAME;
$originHost = ORIGIN_HOST;
$originUsername = ORIGIN_USERNAME;
$originPassword = ORIGIN_PASSWORD;
$originDatabase = ORIGIN_DATABASE;

$destinationHost = DESTINATION_HOST;
$destinationUsername = DESTINATION_USERNAME;
$destinationPassword = DESTINATION_PASSWORD;
$destinationDBName = DESTINATION_DB_NAME;

// Create connections
$destinationDBConnection = new mysqli($destinationHost, $destinationUsername, $destinationPassword, $destinationDBName);

$originDBConnection = new mysqli($originHost, $originUsername, $originPassword, $originDatabase);









//-------------------------------------------------------------------------------------------------------------------------

//Variables

//fetch data from HAPI_RAW_RESERVATIONS and populate associative array
$insertCount = 0;
$updateCount = 0;
$errorCount = 0; // Initialize your error counter at the start of the script

//Actual use of functions, or script logic:





//Pulling import code
$importCode = getFirstNonNullImportCode($originDBConnection, 'hapi_raw_reservations', $errorCount);


//Pull latest EtlTimestamp if exists from PMSDATABASEmisc
try {
    $etlStartTStamp = getLatestEtlTimestamp($destinationDBConnection, $errorCount);
}
catch (Exception $e)
{
echo 'Error: ' . $e->getMessage();
}



try {
    $myDataSemiParsed = fetchDataFromMySQLTable('hapi_raw_reservations', $originDBConnection, $destinationDBConnection, $insertCount, $updateCount, $errorCount);
}
catch (Exception $e)
{
    echo 'Error: ' . $e->getMessage();
}


//Check if myDataSemiParsed is empty.
if (empty($myDataSemiParsed))
    {

        try {
            insertEtlTrackingInfo($destinationDBConnection,$insertCount,$updateCount, $importCode, $schemaVersion, $errorCount);
        }
        catch (Exception $e)
        {
            echo 'Error: ' . $e->getMessage();
        }

        try {
            updateEtlDuration($destinationDBConnection);
        } catch (Exception $e) {
            echo 'Error: ' . $e->getMessage();
        }

        exit();

    }

//Normalized $myDataSemiParsed as it was almost unuseable for some of the heavily nested data

try {
    $normalizedData = normalizeMyDataSemiParsed($myDataSemiParsed);
}
catch (Exception $e)
{
    echo 'Error: ' . $e->getMessage();
}


try {
    insertEtlTrackingInfo($destinationDBConnection,$insertCount,$updateCount, $importCode, $schemaVersion, $errorCount);
}
catch (Exception $e)
{
    echo 'Error: ' . $e->getMessage();
}


//Parse out data from original data source array, $myDataSemiParsed, into arrays based on the final output tables
//// CUSTOMER
///PARENT
try {
    $arrCUSTOMERcontact = createArrCUSTOMERcontact($normalizedData, $errorCount);
}
catch (Exception $e)
{
    echo 'Error: ' . $e->getMessage();
}
 try {
    $arrCUSTOMERlibContactType = createCUSTOMERContactType($errorCount);
}
catch (Exception $e)
{
    echo 'Error: ' . $e->getMessage();
}
 try {
    $arrCUSTOMERlibLoyaltyProgram = createCUSTOMERloyaltyProgram($errorCount);
}
catch (Exception $e)
{
    echo 'Error: ' . $e->getMessage();
}


//print_r($myDataSemiParsed)
/// CHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
/// GRANDCHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
///
//// SERVICES
///PARENT
//$arrSERVICESlibFolioOrdersType = createSERVICESlibFolioOrderType($errorCount);
$arrSERVICESlibTender = createSERVICESlibTender($myDataSemiParsed, $errorCount);
$arrSERVICESlibServiceItems = createSERVICESlibServiceItems($myDataSemiParsed, $errorCount);
/// CHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
/// GRANDCHILD
//can't populate until primary keys for parent tables are established. These are made via a table trigger/stored proc combo
///
//// RESERVATION
///PARENT
$arrRESERVATIONlibProperty = createRESERVATIONLibProperty($myDataSemiParsed, $errorCount);
$arrRESERVATIONlibSource = createRESERVATIONLibSource($myDataSemiParsed, $errorCount);
$arrRESERVATIONlibRoomClass = createRESERVATIONLibRoomClass($myDataSemiParsed, $errorCount);
$arrRESERVATIONlibRoomType = createRESERVATIONLibRoomType($myDataSemiParsed, $errorCount);
$arrRESERVATIONlibStayStatus = createRESERVATIONLibStayStatus($myDataSemiParsed, $errorCount);
$arrRESERVATIONGroup = createRESERVATIONGroup($myDataSemiParsed, $errorCount);
$arrRESERVATIONlibRoom = createRESERVATIONLibRoom($myDataSemiParsed, $errorCount);



//Validate Parent table associative arrays that they would fit into their appropriate tables
// Loop through each table and its corresponding data array


 //Upsert Parent table associative arrays into their appropriate tables

//Upsert into CUSTOMERlibContactType table
try {
   upsertCustomerContactType($arrCUSTOMERlibContactType, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into CUSTOMERcontact table
try {
   upsertCustomerContact($arrCUSTOMERcontact, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into CUSTOMERlibLoyaltyProgram table
try {
   upsertCustomerLibLoyaltyProgram($arrCUSTOMERlibLoyaltyProgram, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into RESERVATIONlibRoom table
try {
   upsertReservationLibRoom($arrRESERVATIONlibRoom, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into RESERVATIONlibSource
try {
   upsertReservationLibSource($arrRESERVATIONlibSource, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibProperty table
try {
   upsertReservationLibProperty($arrRESERVATIONlibProperty, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


//Upsert into SERVICESlibTender table
try {
   upsertServicesLibTender($arrSERVICESlibTender, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into SERVICESlibServiceItems table
try {
   upsertServicesLibServiceItems($arrSERVICESlibServiceItems, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

////Upsert into SERVICESlibFolioOrdersType table
//try {
//   upsertServicesLibFolioOrdersType($arrSERVICESlibFolioOrdersType, $destinationDBConnection, $errorCount);
//} catch (Exception $e) {
//   echo 'Error: ' . $e->getMessage();
//}


//Upsert into RESERVATIONlibGroup table
try {
   upsertReservationGroup($arrRESERVATIONGroup, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibStayStatus table
try {
   upsertReservationLibStayStatus($arrRESERVATIONlibStayStatus, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibRoomType table
try {
   upsertReservationLibRoomType($arrRESERVATIONlibRoomType, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}

//Upsert into RESERVATIONlibRoomClass table
try {
   upsertReservationLibRoomClass($arrRESERVATIONlibRoomClass, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
   echo 'Error: ' . $e->getMessage();
}


// Get Parent table associative arrays  with new primary keys to prepare for upsert of child tables
// Update $arrCUSTOMERlibContactType
$arrCUSTOMERlibContactType = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERlibContactType', $errorCount);
// Update $arrCUSTOMERContact
$arrCUSTOMERcontact = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERcontact', $errorCount);
// Update $arrCUSTOMERlibLoyaltyProgram
$arrCUSTOMERlibLoyaltyProgram = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERlibLoyaltyProgram', $errorCount);
// Update $arrRESERVATIONlibRoom
$arrRESERVATIONlibRoom = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibRoom', $errorCount);
// Update $arrRESERVATIONlibRoomType
$arrRESERVATIONlibRoomType = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONLibRoomType', $errorCount);
//// Update $arrRESERVATIONlibRoomClass
$arrRESERVATIONlibRoomClass = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibRoomClass', $errorCount);
// Update $arrRESERVATIONlibProperty
$arrRESERVATIONlibProperty = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibProperty', $errorCount);
// Update $arrRESERVATIONGroup
$arrRESERVATIONGroup = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONgroup', $errorCount);
// Update $arrRESERVATIONlibsource
$arrRESERVATIONlibSource = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibsource', $errorCount);
//// Update $arrRESERVATIONlibstaystatus
$arrRESERVATIONlibStayStatus = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONlibstaystatus', $errorCount);
// Update $arrSERVICESlibtender
$arrSERVICESlibTender = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibTender', $errorCount);
//// Update $arrSERVICESlibFolioOrdersType
//$arrSERVICESlibFolioOrdersType = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibFolioOrdersType', $errorCount);
// Update $arrRESERVATIONlibProperty
$arrSERVICESlibServiceItems = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibServiceItems', $errorCount);


//Child arrays and tables
// 1) RESERVATIONstay
// 2) RESERVATIONroomDetails
// 3) RESERVATIONstayStatusStay
// 4) CUSTOMERrelationship

// 5) CUSTOMERmembership
// 6) SERVICESpayment
//Create child associative arrays using the populated parent tables and the raw data
// 1) RESERVATIONstay
$arrRESERVATIONstay = createArrRESERVATIONstay($destinationDBConnection,$normalizedData, $arrRESERVATIONlibSource, $arrRESERVATIONlibProperty, $errorCount);
// 2) CUSTOMERrelationship
$arrCUSTOMERrelationship = createArrCUSTOMERrelationship($myDataSemiParsed, $arrCUSTOMERlibContactType, $arrCUSTOMERcontact, $errorCount);
// 3) CUSTOMERmembership
$arrCUSTOMERmembership = createArrCUSTOMERmembership($myDataSemiParsed, $arrCUSTOMERlibLoyaltyProgram, $arrCUSTOMERcontact, $errorCount);
// 4) SERVICESpayment
$arrSERVICESpayment = createArrSERVICESpayment($myDataSemiParsed, $arrSERVICESlibTender, $errorCount);
//Populate child tables
// 1) RESERVATIONstay
//Upsert into RESERVATIONstay table
try {
    upsertReservationStay($arrRESERVATIONstay, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
// 2) CUSTOMERrelationship
try {
    upsertCustomerRelationship($arrCUSTOMERrelationship, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
// 3) CUSTOMERmembership
try {
    upsertCUSTOMERmembership($arrCUSTOMERmembership, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
// 4) SERVICESpayment
try {
    upsertSERVICESpayment($arrSERVICESpayment, $destinationDBConnection, $errorCount);
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
$arrRESERVATIONroomDetails = createArrRESERVATIONroomDetails($normalizedData, $arrCUSTOMERcontact, $arrRESERVATIONstay,$arrRESERVATIONlibRoomType, $arrRESERVATIONlibRoomClass, $arrRESERVATIONlibRoom, $errorCount);
// 2) RESERVATIONstayStatusStay
// First, index $arrRESERVATIONstay
$indexedArrRESERVATIONstay = indexArrReservationStay($arrRESERVATIONstay);

try {
    $arrRESERVATIONstayStatusStay = createArrRESERVATIONstayStatusStay($normalizedData, $arrRESERVATIONstay, $arrRESERVATIONlibStayStatus, $errorCount);
} catch (Exception $e) {
}
// 3) RESERVATIONgroupStay
//May need to put this one on hold until we get actual group data from Hapi =/
//$arrRESERVATIONgroupStay = create_arrRESERVATIONgroupStay($arrRESERVATIONstay, $arrRESERVATIONgroup);
// 4) SERVICESfolioOrders
try {
    $arrSERVICESfolioOrders = createArrSERVICESfolioOrders($normalizedData, $arrCUSTOMERcontact, $arrRESERVATIONstay, $arrSERVICESpayment, $arrSERVICESlibServiceItems, $errorCount);
} catch (Exception $e) {
}
//remove duplicate records
try {
    $arrSERVICESfolioOrders = removeDuplicateOrders($arrSERVICESfolioOrders);
} catch (Exception $e) {
}
//Populate grandchild tables
// 1) RESERVATIONroomDetails
try {
    upsertReservationRoomDetails($arrRESERVATIONroomDetails, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
// 2) RESERVATIONstayStatusStay
try {
    upsertReservationStayStatusStay($arrRESERVATIONstayStatusStay, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}

// 3) RESERVATIONgroupStay
//skipped since HAPI is not offering any group data
// 4) SERVICESfolioOrders
try {
    upsertSERVICESfolioOrders($arrSERVICESfolioOrders, $destinationDBConnection, $errorCount);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
//// Get Grandchild table associative arrays with new primary keys
//// 1) RESERVATIONroomDetails
try {
    $arrRESERVATIONroomDetails = getTableAsAssociativeArray($destinationDBConnection, 'RESERVATIONroomDetails', $errorCount);
} catch (Exception $e) {
}
//// 2) RESERVATIONstayStatusStay
try {
    $arrRESERVATIONstayStatusStay = getTableAsAssociativeArray($destinationDBConnection, 'RESERVATIONstayStatusStay', $errorCount);
} catch (Exception $e) {
}
//// 3) RESERVATIONgroupStay
////skipped since HAPI is not offering any group data
//// 4) SERVICESfolioOrders
try {
    $arrSERVICESfolioOrders = getTableAsAssociativeArray($destinationDBConnection, 'SERVICESfolioOrders', $errorCount);
} catch (Exception $e) {
}

//Populate grandchild tables
//var_dump(array_slice($arrRESERVATIONstay, 0, 10, true));
//
//var_dump(array_slice($arrRESERVATIONstayStatusStay, 0, 10, true));
//print_r($normalizedData);
//print_r($arrRESERVATIONstay);
//print_r($arrSERVICESfolioOrders);
//
//$output = var_export($normalizedData, true);
//$filename = "normalizedData.txt";
//file_put_contents($filename, $output);
//var_dump(array_slice($arrSERVICESfolioOrders, 0, 10, true));
//$csv_file = new SplFileObject('arrSERVICESfolioOrders.csv', 'w');
//
//foreach ($arrSERVICESfolioOrders as $folioOrders) {
//    $csv_file->fputcsv($folioOrders);
//}



try {
    updateEtlDuration($destinationDBConnection, $errorCount);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}


?>
