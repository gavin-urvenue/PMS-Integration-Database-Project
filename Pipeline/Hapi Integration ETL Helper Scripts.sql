delete from CUSTOMERlibContactType;

delete from CUSTOMERcontact;

delete from RESERVATIONlibRoom;

delete from RESERVATIONlibSource;

delete from RESERVATIONlibProperty;

delete from CUSTOMERlibLoyaltyProgram;

delete from SERVICESlibTender;

delete from SERVICESlibServiceItems;

delete from SERVICESlibFolioOrdersType;

delete from SERVICESlibFolioOrdersType;

delete from RESERVATIONgroup;

delete from RESERVATIONlibStayStatus;

delete from RESERVATIONgroup;

delete from RESERVATIONlibRoom;

delete from RESERVATIONlibRoomType;

delete from RESERVATIONlibRoomClass;

delete from PMSDATABASEmisc ;

delete from RESERVATIONstay;

use pms_db;
-- view table data
SELECT * from CUSTOMERlibContactType;

SELECT * from CUSTOMERcontact where firstname = 'UNKNOWN' and lastname = 'UNKNOWN';

SELECT * from CUSTOMERlibLoyaltyProgram clp;

SELECT * from RESERVATIONlibRoom rr ;

SELECT * from RESERVATIONlibSource;

SELECT * from RESERVATIONlibProperty rp ;


SELECT * from SERVICESlibTender st;

SELECT * from SERVICESlibServiceItems ssi;

SELECT * from SERVICESlibFolioOrdersType sfot;

SELECT * from RESERVATIONgroup r 

SELECT * from RESERVATIONlibStayStatus lss

SELECT * from RESERVATIONlibRoomType lrt

SELECT * from RESERVATIONlibRoomClass lrc

Select * from PMSDATABASEmisc p 

Select * from RESERVATIONstay r 

describe RESERVATIONlibStayStatus

-- view table counts

select count(*), 'CUSTOMERlibContactType' tableName from CUSTOMERlibContactType
UNION
select count(*), 'CUSTOMERcontact'  from CUSTOMERcontact
UNION
select count(*), 'CUSTOMERlibLoyaltyProgram'  from CUSTOMERlibLoyaltyProgram
UNION
select count(*), 'SERVICESlibTender'  from SERVICESlibTender lt 
UNION
select count(*), 'SERVICESlibServiceItems'  from SERVICESlibServiceItems lsi 
UNION
select count(*), 'SERVICESlibFolioOrdersType'  from SERVICESlibFolioOrdersType lfo 
UNION
select count(*), 'RESERVATIONlibRoom'  from RESERVATIONlibRoom
UNION
select count(*), 'RESERVATIONlibSource'  from RESERVATIONlibSource rs 
UNION
select count(*), 'RESERVATIONlibProperty'  from RESERVATIONlibProperty rs 
UNION
select count(*), 'RESERVATIONgroup'  from RESERVATIONgroup g 
UNION
select count(*), 'RESERVATIONlibStayStatus'  from RESERVATIONlibStayStatus rss 
UNION
select count(*), 'RESERVATIONlibRoomType'  from RESERVATIONlibRoomType rrt 
UNION
select count(*), 'RESERVATIONlibRoomClass'  from RESERVATIONlibRoomClass rrt 
UNION
select count(*), 'RESERVATIONstay'  from RESERVATIONstay r 

UPDATE mysql.user
SET max_questions = '0'
where User = 'urvenue'

FLUSH PRIVILEGES;
