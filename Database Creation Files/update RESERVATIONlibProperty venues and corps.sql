-- Scipt to update corpEntIds in pms_db database, table RESERVATIONlibProperty
use pms_db;

UPDATE pms_db.RESERVATIONlibProperty
	SET corpEntId = 1857
	where chainCode in ('ACCOR');
	

	
	
UPDATE pms_db.RESERVATIONlibProperty
	SET venueId  = 938176
	where propertyCode in ('BSH');


select * from pms_db.RESERVATIONlibProperty rp 