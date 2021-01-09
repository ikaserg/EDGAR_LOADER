import db_pg as db

def create_tmp_report_rows(p_db):
    v_sql = 'create table if not exists \
               tmp_report_rows(report_id integer, tag_name text, context_ref text, val text)';
    result =  db.execute_query(p_db, v_sql)
    p_db.commit()
    return result

def get_company_by_ecik(p_db, p_ecik):
    v_sql = 'select company_id \
               from tbl_companies \
              where ECIK = %(ecik)s ;'
    result = db.do_query_one(p_db, v_sql, {'ecik': p_ecik})
    if result is not None:
        result = result[0]
    return result

def ins_company(p_db, p_ecik, p_company_name, p_symbol):
    v_sql =  "insert into tbl_companies(company_id, short_name, symbol, ECIK) \
              values (nextval('seq_companies'), %(short_name)s, %(symbol)s, %(ecik)s)  \
              returning company_id;"
    result = db.do_query_one(p_db, v_sql, {'ecik': p_ecik, 'short_name': p_company_name, 'symbol': p_symbol})[0]
    if result is not None:
        result = result[0]
    return result

def get_orins_company(p_db, p_ecik, p_company_name, p_symbol):
    result = get_company_by_ecik(p_db, p_ecik)
    if result is None:
        result = ins_company(p_db, p_ecik, p_company_name, p_symbol)
    return result    

def get_report_by_period(p_db, p_company_id, p_period, p_year, p_type):
    v_sql = 'select report_id \
               from tbl_reports \
              where company_id = %(company_id)s \
                and FiscalPeriodFocus = %(period)s \
                and FiscalYearFocus = %(year)s \
                and DocumentType = %(type)s '
    result = db.do_query_one(p_db, v_sql, {'company_id': p_company_id, 'period': p_period, \
                                           'year': p_year, 'type': p_type})
    if result is not None:
        result = result[0]
    return result

def ins_report(p_db, p_company_id, p_rep):
    v_sql = "insert into tbl_reports(report_id, company_id, FiscalPeriodFocus, FiscalYearFocus, \
                                     DocumentType, FiscalYearEndDate, PeriodEndDate) \
             values (nextval('seq_reports'), %(company_id)s, %(FiscalPeriodFocus)s, %(FiscalYearFocus)s, \
                     %(DocumentType)s, %(FiscalYearEndDate)s, %(PeriodEndDate)s) \
             returning report_id;"

    result = db.do_query_one(p_db, v_sql, {'company_id': p_company_id, 'period': p_rep['period'], \
                'FiscalPeriodFocus': p_rep['period'], 'FiscalYearFocus': p_rep['year'], \
                'DocumentType': p_rep['rtype'], 'FiscalYearEndDate': p_rep['year_end'], \
                'PeriodEndDate': p_rep['period_end']})
    if result is not None:
        result = result[0]
    return result
    
def get_orins_report(p_db, p_company_id, p_rep):
    result = get_report_by_period(p_db, p_company_id, p_rep['period'], p_rep['year'], p_rep['rtype'])
    if result is None:
        result = ins_report(p_db, p_company_id, p_rep)
    return result

def get_rep_param_by_tag(p_db, p_tag_name):
    v_sql = "select tag_id, param_id \
               from tbl_report_param_tags \
              where tag_name = %(tag_name)s;"              
    result = db.do_query_one(p_db, v_sql, {'tag_name': p_tag_name})
    if result is not None:
        result = result[0]
    return result
