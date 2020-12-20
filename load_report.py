from lxml import etree
import db_pg as db
import quote_db

def get_text_by_name(p_node, p_name, p_defvalue):
    if p_node.tag[-len(p_name):] == p_name:
        return p_node.text
    else:
        return p_defvalue

def read_dei_values(p_root, p_valuekeys):
    res = {l_key: None for l_key in p_valuekeys.keys()}
    for l_tag in p_root.xpath("//*[starts-with(name(), 'dei:')]"):
        for l_key, l_value in p_valuekeys.items():
            res[l_key] = get_text_by_name(l_tag, l_value, res[l_key])
    return res        


def parse_edgar_report(p_db, p_file_name):
    v_root = etree.parse(p_file_name)
    v_valuekeys = {'period': 'DocumentFiscalPeriodFocus', 'year': 'DocumentFiscalYearFocus', \
                   'rtype': 'DocumentType', 'symbol': 'TradingSymbol', 'ecik': 'EntityCentralIndexKey',\
                   'company_name': 'EntityRegistrantName', 'year_end': 'CurrentFiscalYearEndDate', \
                   'period_end': 'DocumentPeriodEndDate'}
    v_rep = read_dei_values(v_root, v_valuekeys)
    if v_rep['year'] is None:
        v_rep['year'] = v_rep['period_end'][:4]
    if v_rep['period'] is None:
        v_rep['period'] = v_rep['period_end']
    if v_rep['year_end'][0] == '-':
        v_rep['year_end'] = v_rep['year'] + v_rep['year_end'][1:]
    print(v_rep)
    v_company_id = quote_db.get_orins_company(p_db, v_rep['ecik'], v_rep['company_name'], v_rep['symbol'])
    v_report_id = quote_db.get_orins_report(p_db, v_company_id, v_rep)
    print(v_company_id)
    print(v_report_id)
    return 0

class Report:
    def __init__(self, db):
        self.db = db

        self.sql_find_report = 'select report_id \
                                 from tbl_reports \
                                where company_id = %(company_id)s \
                                  and FiscalPeriodFocus = %(period)s \
                                  and FiscalYearFocus = %(year)s \
                                  and DocumentType = %(type)s '
        self.sql_ins_report =  "insert into tbl_reports(report_id, company_id, \
                                                        FiscalPeriodFocus, FiscalYearFocus, \
                                                        DocumentType, \
                                                        FiscalYearEndDate, PeriodEndDate) \
                                values(nextval('seq_reports'), %(company_id)s, \
                                       %(FiscalPeriodFocus)s, %(FiscalYearFocus)s, \
                                       %(DocumentType)s, \
                                       %(FiscalYearEndDate)s, %(PeriodEndDate)s) \
                                returning report_id;"
        self.sql_find_company = 'select company_id \
                                  from tbl_companies \
                                 where ECIK = %(ecik)s ;'
        self.sql_ins_company =  "insert into tbl_companies(company_id, short_name, \
                                                           symbol, ECIK) \
                                 values(nextval('seq_companies'), %(short_name)s, \
                                        %(symbol)s, %(ECIK)s)  \
                                returning company_id;"
        
        self.sql_find_param = "select tag_id, param_id \
                                 from tbl_report_param_tags \
                                where tag_name = %(tag_name)s;"

        self.sql_ins_param = "insert into tbl_report_param_tags(tag_id, tag_name ) \
                              values (nextval('seq_tags'), %(tag_name)s) \
                               returning tag_id;"

        self.sql_find_report_row = "select count(*) \
                                      from tbl_report_rows \
                                     where report_id = %(report_id)s \
                                       and tag_id = %(tag_id)s \
                                       and context_ref = %(context_ref)s ;"

        self.sql_ins_report_row = "insert into tbl_report_rows(report_id, param_id, \
                                        tag_id, value_str, context_ref ) \
                                   values(%(report_id)s, %(param_id)s,%(tag_id)s, \
                                          %(value_str)s, %(context_ref)s);"   

    def insert_company(self, company_name, symbol, ecik):
        return 0
    
    def get_param_and_tag(self, r):
        if len(r.tag) > 200: 
            print(r.tag)
        p = self.db.do_query_one_params(self.sql_find_param, {'tag_name': r.tag})
        if p is None:
            p = self.db.do_query_one_params(self.sql_ins_param, {'tag_name': r.tag})
            return p, None
        else:
            return p[0], p[1]    

    def insert_row(self, report_id, r):
        tag_id, param_id = self.get_param_and_tag(r)
        if 'context_ref' in r.attrib:
            context_ref = r.attrib['contextRef']
        else:
            context_ref = ' '    
        if self.db.do_query_one_params(self.sql_find_report_row, {'report_id': report_id, \
                                                                  'tag_id': tag_id,
                                                                  'context_ref': context_ref})[0] == 0:
            self.db.exec_query_params(self.sql_ins_report_row, {'report_id': report_id, \
                                                                'param_id': param_id,
                                                                'tag_id': tag_id,
                                                                'value_str': r.text,
                                                                'context_ref': context_ref})

    def parse_report(self, file_name):
        root = etree.parse(file_name)
        year = None
        period = None
        symbol = None
        for r in root.xpath("//*[starts-with(name(), 'dei:')]"):
            if (r.tag[36:] == 'DocumentFiscalPeriodFocus')  or (r.tag[31:] == 'DocumentFiscalPeriodFocus'):
                period = r.text
            elif (r.tag[36:] == 'DocumentFiscalYearFocus') or (r.tag[31:] == 'DocumentFiscalYearFocus'):
                year = r.text
            elif (r.tag[36:] == 'DocumentType') or (r.tag[31:] == 'DocumentType'):       
                rtype = r.text
            elif (r.tag[36:] == 'TradingSymbol') or (r.tag[31:] == 'TradingSymbol'):
                symbol = r.text
            elif (r.tag[36:] == 'EntityCentralIndexKey') or (r.tag[31:] == 'EntityCentralIndexKey'):
                ecik = r.text    
            elif (r.tag[36:] == 'EntityRegistrantName') or (r.tag[31:] == 'EntityRegistrantName'):
                company_name = r.text
            elif (r.tag[36:] == 'CurrentFiscalYearEndDate') or (r.tag[31:] == 'CurrentFiscalYearEndDate'):
                year_end = r.text    
            elif (r.tag[36:] == 'DocumentPeriodEndDate') or (r.tag[31:] == 'DocumentPeriodEndDate'):
                period_end = r.text
        if year is None:
            year = period_end[:4]
        if period is None:
            period = period_end
        if year_end[0] == '-':
            year_end = year + year_end[1:]

        company_id = self.db.do_query_one_params(self.sql_find_company, {'ecik': ecik})
        if company_id is None:
            company_id = self.insert_company(company_name, symbol, ecik)

        report_id = self.db.do_query_one_params(self.sql_find_report, {'company_id': company_id,\
                                                           'period': period,\
                                                           'year': year,\
                                                           'type': rtype})
        if report_id is None:
            report_id = self.db.do_query_one_params(self.sql_ins_report, {'company_id': company_id, \
                                                              'FiscalPeriodFocus': period,\
                                                              'FiscalYearFocus': year,\
                                                              'DocumentType': rtype,\
                                                              'FiscalYearEndDate': year_end,\
                                                              'PeriodEndDate': period_end})
        self.db.commit()
        for r in root.xpath("//*[starts-with(name(), 'us-gaap:')]"):
            if r.text is not None:
                if r.tag[-9:] != 'TextBlock' and r.text[:5] != '<div>':
                    if len(r.text) > 200:
                        print(r.tag)
                        print(r.text)
                        print(r.attrib)
                    else:    
                        self.insert_row(report_id, r)
        self.db.commit()

v_db = db.get_db_connect()
parse_edgar_report(v_db, "C:\\Data\\edgar\\A\\10-K_2009-10-31_091252724\\a-20091031.xml")
                        