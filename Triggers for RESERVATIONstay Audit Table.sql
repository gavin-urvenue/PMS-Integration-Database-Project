-- Triggers for RESERVATIONstay audit table:

-- Insert Trigger
CREATE TRIGGER after_insert_RESERVATIONstay
AFTER INSERT ON RESERVATIONstay
FOR EACH ROW
BEGIN
  INSERT INTO RESERVATIONstay_audit (operation_type, operated_by, id, status, rndCode, createTStamp, modTStamp, createDateTime, modifyDateTime, startDate, endDate, createdBy, metaData, extPMSConfNum, extGuestId, dataSource, libSourceId, libPropertyId)
  VALUES ('INSERT', USER(), NEW.id, NEW.status, NEW.rndCode, NEW.createTStamp, NEW.modTStamp, NEW.createDateTime, NEW.modifyDateTime, NEW.startDate, NEW.endDate, NEW.createdBy, NEW.metaData, NEW.extPMSConfNum, NEW.extGuestId, NEW.dataSource, NEW.libSourceId, NEW.libPropertyId);
END;

-- Update Trigger
CREATE TRIGGER after_update_RESERVATIONstay
AFTER UPDATE ON RESERVATIONstay
FOR EACH ROW
BEGIN
  INSERT INTO RESERVATIONstay_audit (operation_type, operated_by, id, status, rndCode, createTStamp, modTStamp, createDateTime, modifyDateTime, startDate, endDate, createdBy, metaData, extPMSConfNum, extGuestId, dataSource, libSourceId, libPropertyId)
  VALUES ('UPDATE', USER(), NEW.id, NEW.status, NEW.rndCode, NEW.createTStamp, NEW.modTStamp, NEW.createDateTime, NEW.modifyDateTime, NEW.startDate, NEW.endDate, NEW.createdBy, NEW.metaData, NEW.extPMSConfNum, NEW.extGuestId, NEW.dataSource, NEW.libSourceId, NEW.libPropertyId);
END;

-- Delete Trigger
CREATE TRIGGER before_delete_RESERVATIONstay
BEFORE DELETE ON RESERVATIONstay
FOR EACH ROW
BEGIN
  INSERT INTO RESERVATIONstay_audit (operation_type, operated_by, id, status, rndCode, createTStamp, modTStamp, createDateTime, modifyDateTime, startDate, endDate, createdBy, metaData, extPMSConfNum, extGuestId, dataSource, libSourceId, libPropertyId)
  VALUES ('DELETE', USER(), OLD.id, OLD.status, OLD.rndCode, OLD.createTStamp, OLD.modTStamp, OLD.createDateTime, OLD.modifyDateTime, OLD.startDate, OLD.endDate, OLD.createdBy, OLD.metaData, OLD.extPMSConfNum, OLD.extGuestId, OLD.dataSource, OLD.libSourceId, OLD.libPropertyId);
END;
