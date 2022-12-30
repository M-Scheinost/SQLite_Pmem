//tatp_transaction
// This is the target database transaction file of the TATP Benchmark Suite.
// For DB2.

GET_SUBSCRIBER_DATA {
	SELECT s_id, sub_nbr, 
		bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7, bit_8, bit_9, bit_10,
		hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7, hex_8, hex_9, hex_10,
		byte2_1, byte2_2, byte2_3, byte2_4, byte2_5, byte2_6, byte2_7,
		byte2_8, byte2_9, byte2_10,
		msc_location, vlr_location
	FROM subscriber 
	WHERE s_id = <s_id rnd>;
} 

GET_NEW_DESTINATION {
	SELECT cf.numberx
	FROM special_facility AS sf, 
		call_forwarding AS cf
	WHERE (sf.s_id = <s_id rnd> AND sf.sf_type = <sf_type rnd> AND sf.is_active = 1)
		AND (cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type)
		AND (cf.start_time \<= <start_time rnd> AND <end_time rnd> \< cf.end_time);
} 

GET_ACCESS_DATA {
	SELECT data1, data2, data3, data4
	FROM access_info 
	WHERE s_id = <s_id rnd> 
              AND ai_type = <ai_type rnd>;
} 

UPDATE_SUBSCRIBER_DATA {
	UPDATE subscriber 
	SET bit_1 = <bit rnd>
	WHERE s_id = <s_id rnd subid>;

	UPDATE special_facility
	SET data_a = <data_a rnd>
	WHERE s_id = <s_id value subid>
              AND sf_type = <sf_type rnd>;
} (ERRORS ALLOWED 23503)

UPDATE_LOCATION {
	UPDATE subscriber
	SET vlr_location = <vlr_location rnd> 
	WHERE sub_nbr = <sub_nbr rndstr>;
} (ERRORS ALLOWED 23505)

DELETE_CALL_FORWARDING {
	SELECT <s_id bind subid s_id> 
	FROM subscriber
	WHERE sub_nbr = <sub_nbr rndstr>;

	DELETE 
	FROM call_forwarding
	WHERE s_id = <s_id value subid> 
	      AND sf_type = <sf_type rnd> 
              AND start_time = <start_time rnd>;
} (ERRORS ALLOWED 23503) 

INSERT_CALL_FORWARDING {
	SELECT <s_id bind subid s_id>
	FROM subscriber 
	WHERE sub_nbr = <sub_nbr rndstr>;

	SELECT <sf_type bind sfid sf_type> 
	FROM special_facility
	WHERE s_id = <s_id value subid>;

	INSERT INTO call_forwarding 
	VALUES (<s_id value subid>, <sf_type rnd sf_type>, 
		<start_time rnd>, <end_time rnd>, <numberx rndstr>);
} (ERRORS ALLOWED 23503, 23505)

// Accepted error codes used above:
// minus sign is not allowed
// -803 [IBM][CLI Driver][DB2/NT] SQL0803N  One or more values in the INSERT statement, UPDATE statement, or foreign key update caused by a DELETE statement are not valid because the primary key, unique constraint or unique index identified by "1" constrains table "TESTSYSTEM.CALL_FORWARDING" from having duplicate values for the index key.  SQLSTATE=23505
// -530 [IBM][CLI Driver][DB2/NT] SQL0530N  The insert or update value of the FOREIGN KEY "TESTSYSTEM.CALL_FORWARDING.SQL080218182918000" is not equal to any value of the parent key of the parent table.  SQLSTATE=23503
