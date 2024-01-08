/*Special Request from Wayne!
 * Every reservation in theory should have a guest ID attached to it which is the ID of the main guest
 * attached to the reservation. Thus, why not have a guest ID field in the RESERVATIONstay field so that
 * one could obtain it without having to join tables? These triggers accomplish this!
 * Here we have a stored procedure that accomplishes this, and 2 triggers that call the stored procedure
 * whenever RESERVATIONstay is updated or inserted into.
 * 
 * v1.0 - Initial Creation
 *
 * Update 1/8/2024, this doesn't work on Urvenue databases because the cdc triggers partnered with these triggers create an endless loop. Need to put the stored procedure in here in a job.
 **/

DELIMITER //

CREATE PROCEDURE updateExtGuestID(IN newStayID INT)
BEGIN
    DECLARE guestID INT;

    -- Select the guest ID from CUSTOMERcontact based on the new stay ID
    SELECT c.extGuestID INTO guestID
    FROM CUSTOMERcontact c 
    INNER JOIN ReservationStay s ON c.id = s.contactId
    WHERE fo.stayid = newStayID;

    -- Update the RESERVATIONstay table with the guest ID
    UPDATE RESERVATIONstay
    SET extGuestID = guestID
    WHERE id = newStayID;
END;

//
DELIMITER ;

DELIMITER //

CREATE TRIGGER extGuestID_query_populate_insert
AFTER INSERT ON RESERVATIONstay
FOR EACH ROW
BEGIN
    CALL updateExtGuestID(NEW.id);
END;

//
DELIMITER ;

DELIMITER //

CREATE TRIGGER extGuestID_query_populate_update
AFTER UPDATE ON RESERVATIONstay
FOR EACH ROW
BEGIN
    CALL updateExtGuestID(NEW.id);
END;

//
DELIMITER ;

