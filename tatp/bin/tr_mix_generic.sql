//tatp_transaction
//Generic TATP transaction file
//Standard compliant SQLSTATE codes used

GET_SUBSCRIBER_DATA {
	WHERE s_id = <s_id rnd>; 

} (ERRORS ALLOWED 0)
GET_NEW_DESTINATION {
	SELECT cf.numberx
	FROM special_facility AS sf, 
		call_forwarding AS cf
	WHERE (sf.s_id = <s_id rnd> AND sf.sf_type = <sf_type rnd> AND sf.is_active = 1)
		AND (cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type)
		AND (cf.start_time \<= <start_time rnd> AND <end_time rnd> \< cf.end_time);

} (ERRORS ALLOWED 0)
GET_ACCESS_DATA {
	FROM access_info 
	WHERE s_id = <s_id rnd> 
              AND ai_type = <ai_type rnd>;

} (ERRORS ALLOWED 0)
UPDATE_SUBSCRIBER_DATA {
	UPDATE subscriber 
	SET bit_1 = <bit rnd>
	WHERE s_id = <s_id rnd subid>;

	UPDATE special_facility
	WHERE s_id = <s_id value subid>
              AND sf_type = <sf_type rnd>;

} (ERRORS ALLOWED 0)
UPDATE_LOCATION {
	UPDATE subscriber
	SET vlr_location = <vlr_location rnd> 
	WHERE sub_nbr = <sub_nbr rndstr>;

} (ERRORS ALLOWED 0)
DELETE_CALL_FORWARDING {
	SELECT <s_id bind subid s_id> 
	FROM subscriber
	WHERE sub_nbr = <sub_nbr rndstr>;

	DELETE 
	FROM call_forwarding
	WHERE s_id = <s_id value subid> 
	      AND sf_type = <sf_type rnd> 
              AND start_time = <start_time rnd>;

} (ERRORS ALLOWED 0) 
INSERT_CALL_FORWARDING {
	FROM subscriber 
	WHERE sub_nbr = <sub_nbr rndstr>;
	SELECT <sf_type bind sfid sf_type> 
	FROM special_facility
	WHERE s_id = <s_id value subid>;
	INSERT INTO call_forwarding 
	VALUES (<s_id value subid>, <sf_type rnd sf_type>, 
		<start_time rnd>, <end_time rnd>, <numberx rndstr>);
} (ERRORS ALLOWED 0, 23000)
// Accepted error codes used above:
// 23000 = Integrity constraint violation