
-- delete grandchild records
delete from RESERVATIONgroupStay ;

delete from RESERVATIONstayStatusStay ;

delete from RESERVATIONroomDetails ;

delete from SERVICESfolioOrders ;

-- delete child records
delete from CUSTOMERrelationship ;


delete from RESERVATIONstay;

delete from CUSTOMERmembership ;

delete from SERVICESpayment ;


-- delete parent records
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

-- Stored procedure that completes all of the deletes above
CALL pms_db.usp_clear_all_tables();

select * from RESERVATIONstay 


    SELECT *
    FROM CUSTOMERcontact c 
    INNER JOIN Reservationstay s ON c.id = s.contactId
    WHERE fo.stayid = 4085078;
   
select * from SERVICESfolioOrders so 

use pms_db;
-- view table data
SELECT * from CUSTOMERlibContactType;

SELECT * from CUSTOMERcontact where firstname = 'Mark';

SELECT * from CUSTOMERlibLoyaltyProgram clp;

SELECT * from RESERVATIONlibRoom rr ;

SELECT * from RESERVATIONlibSource;

SELECT * from RESERVATIONlibProperty rp ;


SELECT * from SERVICESlibTender st;

SELECT * from SERVICESlibServiceItems ssi;

SELECT * from SERVICESlibFolioOrdersType sfot;

SELECT * from RESERVATIONgroup r 

SELECT * from RESERVATIONstay r;

SELECT * from RESERVATIONlibStayStatus lss

SELECT * from RESERVATIONlibRoomType lrt

SELECT * from RESERVATIONlibRoomClass lrc

Select * from PMSDATABASEmisc p 
SHOW VARIABLES LIKE 'log_error';

Select * from RESERVATIONstay r where startDate = '2022-11-28' AND endDate = '2022-12-12'

Select * from CUSTOMERrelationship c 

Select * from CUSTOMERmembership c 

Select * from SERVICESpayment s 

Select * from RESERVATIONstayStatusStay rss 

SELECT COUNT(*) as count FROM SERVICESpayment WHERE paymentAmount = 0 AND currencyCode is NULL AND dataSource = 'HAPI' AND libTenderId = 4031159

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
UNION
select count(*), 'CUSTOMERrelationship'  from CUSTOMERrelationship c  
UNION
select count(*), 'CUSTOMERmembership'  from CUSTOMERmembership c2 
UNION
select count(*), 'SERVICESpayment'  from SERVICESpayment s

UPDATE mysql.user
SET max_questions = '0'
where User = 'urvenue'

FLUSH PRIVILEGES;
