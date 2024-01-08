<?php
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

//Normalized $myDataSemiParsed as it was almost unuseable for some of the heavily nested data

try {
    $normalizedData = normalizeMyDataSemiParsed($myDataSemiParsed);
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
$arrCUSTOMERcontact = createCUSTOMERcontact($myDataSemiParsed);
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
        $arrCUSTOMERcontact,
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
   upsertCustomerContact($arrCUSTOMERcontact, $destinationDBConnection);
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
$arrCUSTOMERcontact = getTableAsAssociativeArray($destinationDBConnection,'CUSTOMERcontact');
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
$arrSERVICESlibFolioOrdersType = getTableAsAssociativeArray($destinationDBConnection,'SERVICESlibFolioOrdersType');
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
//Create child associative arrays using the populated parent tables and the raw data
// 1) RESERVATIONstay
$arrRESERVATIONstay = createArrReservationStay($destinationDBConnection,$myDataSemiParsed, $arrRESERVATIONlibSource, $arrRESERVATIONlibProperty);
// 2) CUSTOMERrelationship
$arrCUSTOMERrelationship = createArrCUSTOMERrelationship($myDataSemiParsed, $arrCUSTOMERlibContactType, $arrCUSTOMERcontact);
// 3) CUSTOMERmembership
$arrCUSTOMERmembership = createArrCUSTOMERmembership($myDataSemiParsed, $arrCUSTOMERlibLoyaltyProgram, $arrCUSTOMERcontact);
// 4) SERVICESpayment
$arrSERVICESpayment = createArrSERVICESpayment($myDataSemiParsed, $arrSERVICESlibTender);
//Populate child tables
// 1) RESERVATIONstay
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
$arrRESERVATIONroomDetails = createArrReservationRoomDetails($myDataSemiParsed, $arrCUSTOMERcontact, $arrRESERVATIONstay,$arrRESERVATIONlibRoomType, $arrRESERVATIONlibRoomClass, $arrRESERVATIONlibRoom);
// 2) RESERVATIONstayStatusStay
// First, index $arrRESERVATIONstay
$indexedArrRESERVATIONstay = indexArrReservationStay($arrRESERVATIONstay);

$arrRESERVATIONstayStatusStay = createArrRESERVATIONstayStatusStay($myDataSemiParsed, $arrRESERVATIONstay, $arrRESERVATIONlibStayStatus);
// 3) RESERVATIONgroupStay
//May need to put this one on hold until we get actual group data from Hapi =/
//$arrRESERVATIONgroupStay = create_arrRESERVATIONgroupStay($arrRESERVATIONstay, $arrRESERVATIONgroup);
// 4) SERVICESfolioOrders
$arrSERVICESfolioOrders = createArrSERVICESfolioOrders($normalizedData, $arrCUSTOMERcontact, $arrRESERVATIONstay, $arrSERVICESpayment, $arrSERVICESlibServiceItems, $arrSERVICESlibFolioOrdersType);
//remove duplicate records
$arrSERVICESfolioOrders = removeDuplicateOrders($arrSERVICESfolioOrders);
//Populate grandchild tables
// 1) RESERVATIONroomDetails
try {
    upsertReservationRoomDetails($arrRESERVATIONroomDetails, $destinationDBConnection);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
// 2) RESERVATIONstayStatusStay
try {
    upsertReservationStayStatusStay($arrRESERVATIONstayStatusStay, $destinationDBConnection);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}

// 3) RESERVATIONgroupStay
//skipped since HAPI is not offering any group data
// 4) SERVICESfolioOrders
try {
    upsertSERVICESfolioOrders($arrSERVICESfolioOrders, $destinationDBConnection);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
// Get Grandchild table associative arrays with new primary keys
// 1) RESERVATIONroomDetails
$arrRESERVATIONroomDetails = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONroomDetails');
// 2) RESERVATIONstayStatusStay
$arrRESERVATIONstayStatusStay = getTableAsAssociativeArray($destinationDBConnection,'RESERVATIONstayStatusStay');
// 3) RESERVATIONgroupStay
//skipped since HAPI is not offering any group data
// 4) SERVICESfolioOrders
$arrSERVICESfolioOrders = getTableAsAssociativeArray($destinationDBConnection,'SERVICESfolioOrders');

//Populate grandchild tables
//print('$arrCUSTOMERcontact:');
//var_dump(array_slice($arrSERVICESfolioOrders, 0, 30, true));


try {
    updateEtlDuration($destinationDBConnection);
} catch (Exception $e) {
    echo 'Error: ' . $e->getMessage();
}
?>
