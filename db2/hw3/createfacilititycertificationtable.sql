drop table cse532.facilitycertification;

CREATE TABLE cse532.facilitycertification (
FacilityID VARCHAR(16),
FacilityName VARCHAR(128),
Description VARCHAR(128),
AttributeType VARCHAR(64),
AttributeValue VARCHAR(64),
MeasureValue VARCHAR(64), 
County VARCHAR(16)
);

load from "./Health_Facility_Certification_Information.csv" of del MESSAGES load.msg INSERT INTO cse532.facilitycertification;