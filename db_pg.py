import psycopg2
import configparser

def get_db_connect(p_filename):
	v_cfg = configparser.ConfigParser()
	v_cfg.read(p_filename)
	v_str = "host={host} dbname={dbname} user={user} password={pwd} port={port}"
	return psycopg2.connect(v_str.format(host=v_cfg['connect']['host'], dbname=v_cfg['connect']['dbname'],\
		       user=v_cfg['connect']['user'], pwd=v_cfg['connect']['password'], port=v_cfg['connect']['port']  ))


def execute_query(p_db, p_sql, p_params = None):
	v_cur = p_db.cursor()
	if p_params is None:
		v_cur.execute(p_sql) 	
	if p_params is not None:
		v_cur.execute(p_sql, p_params)
	return v_cur

def do_query_one(p_db, p_sql, p_params = None):
	v_cur = execute_query(p_db, p_sql, p_params)
	result = v_cur.fetchone()
	v_cur.close()
	return result

def do_exec_query(p_db, p_sql, p_params = None):
	v_cur = execute_query(p_db, p_sql, p_params)
	v_cur.close()
	return 0

