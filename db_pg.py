import psycopg2

def execute_query(p_db, p_sql, p_params):
	v_cur = p_db.cursor()
	if p_params is None:
		v_cur.execute(p_sql) 	
	if p_params is not None:
		v_cur.execute(p_sql, p_params)
	return v_cur

def do_query_one(p_db, p_sql, p_params):
	v_cur = execute_query(p_db, p_sql, p_params)
	result = v_cur.fetchone()
	v_cur.close()
	return result

def do_exec_query(p_db, p_sql, p_params):
	v_cur = execute_query(p_db, p_sql, p_params)
	v_cur.close()
	return 0

