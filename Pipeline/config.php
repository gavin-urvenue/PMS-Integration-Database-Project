<?php

//Constants
    //schema aversion
define('SCHEMA_VERSION', '1.44');
// Database Connections:
    //origin
define('ORIGIN_TABLE_NAME', 'hapi_raw_reservations');
define('ORIGIN_HOST', 'localhost:3306');
define('ORIGIN_USERNAME', 'urvenue');
define('ORIGIN_PASSWORD', 'Password1!');
define('ORIGIN_DATABASE', 'Testing');
    //destination
define('DESTINATION_HOST', 'localhost:3306');
define('DESTINATION_USERNAME', 'urvenue');
define('DESTINATION_PASSWORD', 'Password1!');
define('DESTINATION_DB_NAME', 'pms_db');

//Miscellaneous
define('DAYS_TO_KEEP_LOGS', 3);

//RESERVATIONlibProperty Table corpEntIds and VenueIds dictionaries for reference
const CHAIN_CODE_TO_CORP_ENT_ID_DICT = [
    'ACCOR' => 1857
];

const PROPERTY_CODE_TO_VENUE_ID_DICT = [
    'PGH' => 938176
];
?>