//tatp_transaction
GET_SUBSCRIBER_DATA {
} (ERRORS ALLOWED 0)GET_NEW_DESTINATION {
} (ERRORS ALLOWED 0)GET_ACCESS_DATA {
              AND ai_type = <ai_type rnd>;
} (ERRORS ALLOWED 0)UPDATE_SUBSCRIBER_DATA {
} (ERRORS ALLOWED 0)UPDATE_LOCATION {
} (ERRORS ALLOWED 0)DELETE_CALL_FORWARDING {
	FROM subscriber 
	FROM special_facility
	WHERE s_id = <s_id value subid>;	INSERT INTO call_forwarding 
	VALUES (<s_id value subid>, <sf_type rnd sf_type>, <start_time rnd>, <end_time rnd>, <numberx rndstr>);} (ERRORS ALLOWED 1,2291)// Error codes to be ignored:// 1 23000 [Oracle][ODBC][Ora]ORA-00001: the unique constraint is violated
// 2291 23000 [Oracle][ODBC][Ora]ORA-02291: referential integrity error: no key value in the referenced table.