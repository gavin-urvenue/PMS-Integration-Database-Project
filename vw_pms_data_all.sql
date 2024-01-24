#Comparison View

CREATE VIEW vw_pms_data_all AS
Select  s.createDateTime, s.modifyDateTime, s.startDate stayStartDate, s.endDate stayEndDate, s.createdBy, s.MetaData stayMetaData, s.extPMSConfNum, s.extGuestId, s.dataSource, rrc.className, rr.roomNumber, rrt.typeName, rrt.typeCode,
rss.status, rss.statusName, rs.sourceName, rs.sourceType, rp.chainCode, rp.propertyCode, fo.amount, fo.fixedChargesQuantity, fo.ratePlanCode folioOrderRatePlanCode, fo.isIncluded, 
fo.startDate folioOrderStartDate, fo.endDate folioOrderEndDate, fo.folioOrderType, fo.unitCount, fo.unitPrice, fo.metaData folioOrderMetaData, p.paymentAmount,  p.currencyCode, t.paymentMethod,
si.itemName, si.itemCode, si.ratePlanCode serviceItemRatePlanCode, fot.orderType, c.firstName, c.lastName, c.title, c.email, c.birthDate, c.languageCode, c.languageFormat, c.extGuestId customerGuestId,
c.metaData customerMetaData, r.isPrimaryGuest, ct.`type`, m.`level`, m.membershipCode, lp.name, lp.source 
from pms_db.RESERVATIONstay s
left join RESERVATIONroomDetails rd  on s.id = rd.stayId
left join RESERVATIONlibRoomClass rrc on rd.libRoomClassId  = rrc.id 
left join RESERVATIONlibRoom rr on rd.libRoomId = rr.id
left join RESERVATIONlibRoomType rrt on rd.libRoomTypeId = rrt.id 
left join RESERVATIONstayStatusStay rsss on rd.id = rsss.stayId 
left join RESERVATIONlibStayStatus rss on rsss.stayStatusId = rss.id
left join RESERVATIONlibSource rs on s.libSourceId = rs.id 
left join RESERVATIONlibProperty rp on s.libPropertyId = rp.id
left join SERVICESfolioOrders fo on s.id = fo.stayId 
left join SERVICESpayment p on p.id = fo.paymentId 
left join SERVICESlibTender t on t.id = p.libTenderId
left join SERVICESlibServiceItems si on fo.libServiceItemsId = si.id
left join SERVICESlibFolioOrdersType fot on fo.libFolioOrdersTypeId = fot.id 
left join CUSTOMERcontact c on fo.contactId = c.id 
left join CUSTOMERrelationship r on r.contactId = c.id 
left join CUSTOMERlibContactType ct on r.contactTypeId  = ct.id 
left join CUSTOMERmembership m on m.contactId  = c.id 
left join CUSTOMERlibLoyaltyProgram lp on lp.id = m.libLoyaltyProgramId ;
