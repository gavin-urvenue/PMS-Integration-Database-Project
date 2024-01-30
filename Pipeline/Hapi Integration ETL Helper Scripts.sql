
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


delete from RESERVATIONgroup;

delete from RESERVATIONlibStayStatus;

delete from RESERVATIONgroup;

delete from RESERVATIONlibRoom;

delete from RESERVATIONlibRoomType;

delete from RESERVATIONlibRoomClass;

delete from PMSDATABASEmisc ;

-- Stored procedure that completes all of the deletes above
CALL pms_db.usp_clear_all_tables();

-- view table counts

select count(*), 'CUSTOMERlibContactType' tableName from CUSTOMERlibContactType
UNION
select count(*), 'CUSTOMERcontact'  from CUSTOMERcontact
UNION
select count(*), 'CUSTOMERlibLoyaltyProgram'  from CUSTOMERlibLoyaltyProgram
UNION
select count(*), 'CUSTOMERrelationship'  from CUSTOMERrelationship c  
UNION
select count(*), 'CUSTOMERmembership'  from CUSTOMERmembership c2 
UNION
select count(*), 'SERVICESlibTender'  from SERVICESlibTender lt 
UNION
select count(*), 'SERVICESlibServiceItems'  from SERVICESlibServiceItems lsi 
UNION
select count(*), 'SERVICESpayment'  from SERVICESpayment s
UNION
select count(*), 'SERVICESfolioOrders' from SERVICESfolioOrders so 
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
select count(*), 'RESERVATIONroomDetails' from RESERVATIONroomDetails rd 
UNION
select count(*), 'RESERVATIONstayStatusStay' from RESERVATIONstayStatusStay rss 
UNION
select count(*), 'RESERVATIONgroupStay' from RESERVATIONgroupStay rs2 
UNION
select count(*), 'PMSDATABASEmisc' from PMSDATABASEmisc p 

select * from pms_db.PMSDATABASEmisc p 



-- update system user to be able to run many queries for pipeline process

UPDATE mysql.user
SET max_questions = 0
where User = urvenue

FLUSH PRIVILEGES;



