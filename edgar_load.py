from bs4 import BeautifulSoup
import requests
import psycopg2
import zipfile
import os
import pandas as pd
import datetime
import time

#-------------------------------------------------------------------------------------------
#   function for load Standart industries codes
#-------------------------------------------------------------------------------------------
def get_sic_list_url():
    return 'https://www.sec.gov/info/edgar/siccodes.htm'
    
def parse_sic_list(url):
    result = []
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    for tr in soup.findAll("table", {"class": "sic"})[0].findAll("tr"):
        tds = tr.findChildren("td", recursive = False)
        if (len(tds) > 0):
            result.append({"sic": tds[0].text, "office": tds[1].text, "name": tds[2].text})
    return result
#-------------------------------------------------------------------------------------------
#       functions for DB layer
def insert_sic(db, sic):
    db.execute("insert into tbl_std_ind_codes(sic, office, name) values(%s, %s, %s);", (sic['sic'], sic['office'], sic['name']))

def insert_sic_list(db, sic_list):
    for sic in sic_list:
        insert_sic(db, sic)
        db.connection.commit()  
#-------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------
#   function for download companies reports
#-------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------
#       URL builders

def get_CIK_url(ticker, start = 0, count = 100):
    return 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=&dateb=&owner=include&start={1}&count={2}'.format(ticker, str(start), str(count))

def get_ticker_url(ticker, start = 0, count = 100):
    return 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=&dateb=&owner=include&start={1}&count={2}'.format(ticker, str(start), str(count))

#-------------------------------------------------------------------------------------------
#       work with report 

def parse_report_list_page(soup):
    result = []
    for tr in soup.findAll("table", {"class": "tableFile2"})[0].findAll("tr"):
        tds = tr.findChildren("td", recursive = False)
        if (len(tds) > 0):
            if(tds[0].text in ['10-K', '10-Q']):
                rpt = {}
                rpt['type'] = tds[0].text
                rpt['url'] = tr.findAll("a", {"id": "documentsbutton"})[0]['href']
                rpt['date'] = tds[3].text
                rpt['file_num'] = tds[4].findAll("a")[0].text
                rpt['film_num'] = tds[4].text[len(rpt['file_num']):].rstrip()
                result.append(rpt)
    return result

def has_next_button(soup, cnt = 100):
    return len(soup.findAll("input", {"type": "button", "value": "Next "+str(cnt)})) > 0

def extract_addr(tags, mail_type):
    result = ''
    for tag in tags:
        if(tag.text.startswith(mail_type)):
            result = ', '.join([t.text[1:].rstrip() for t in soup.findAll("span", {"class": "mailerAddress"})])
    return result        

def parse_company_info(soup):
    result = {}
    inf = soup.findAll("div", {"class": "companyInfo"})[0]
    txt = inf.findAll("span",{"class": "companyName"})[0].text
    spl = txt.find(' CIK#: ')
    result['name'] = txt[:spl]
    result['cik'] = txt[spl + 7: spl + 7 + 10]
    txt = inf.findAll("p",{"class": "identInfo"})[0].text
    result['sic'] = txt[txt.find(': ') + 2: txt.find(' - ')]        
    mailers = soup.findAll("div", {"class": "mailer"})
    result['legal_addr'] = extract_addr(mailers, 'Business Address')
    result['mail_addr'] = extract_addr(mailers, 'Mailing Address')
    return result

def parse_report_info(soup):
    result = {}
    result['form'] = soup.findAll("div", {"id": "formName"})[0].findAll("strong")[0].text[5:]
    heads = soup.findAll("div", {"class": "formGrouping"})
    result['filing'] = heads[0].findAll("div", string = 'Filing Date')[0].find_next_sibling("div", {"class": "info"}).text
    result['report'] = heads[1].findAll("div", string = 'Period of Report')[0].find_next_sibling("div", {"class": "info"}).text
    return result

def get_report_list(ticker):
    result = []
    has_next = True
    start = 0
    count = 100
    while has_next:        
        url = get_ticker_url(ticker, start, count)
        soup = BeautifulSoup(requests.get(url).content, 'html.parser')
        try:
            lst = parse_report_list_page(soup)                
            if (len(lst) > 0):
                result = result + lst
            has_next = has_next_button(soup, count)
        except:
            print('Error: ' + url) 
            has_next = False
        start = start + count            
    return result

#-------------------------------------------------------------------------------------------
#       work with report files

def get_file_list(soup, url_base):
    result = []
    tbl = soup.findAll("table", {"class": "tableFile", "summary": "Data Files"})
    if len(tbl):
        trs = tbl[0].findAll("tr")
        for tr in trs:
            tds = tr.findAll("td")
            if (len(tds) > 0):
                result.append({"name": tds[2].text.rstrip(), "url": url_base + tds[2].findAll("a")[0]["href"]})
    return result

def download_file(url, file_name, zp = None):
    data = requests.get(url).content
    file = open(file_name, 'wb')
    file.write(data)
    file.close()
    if (zp is not None):
        zp.write(file_name)
        os.remove(file_name)
    return 0    
        
def download_report_files(url, url_base, path, name, compress, skip):    
    soup = BeautifulSoup(requests.get(url_base + url).content, 'html.parser')
    try:
        zp = None
        if compress:
            rep = parse_report_info(soup)
            has_file = os.path.exists(os.path.join(path, rep['form'] +'_'+ rep['report']+'_'+ name + '.zip'))
            if (not has_file):
                zp = zipfile.ZipFile(os.path.join(path, rep['form'] +'_'+ rep['report']+'_'+ name + '.zip'), 'w')
        if (not(has_file & skip)):
            for fl in get_file_list(soup, url_base):
                download_file(fl['url'], fl['name'], zp)
    finally:
        if (zp is not None):
            zp.close()
    return 0    

def download_ticker_reports(ticker, path, base_url = 'https://www.sec.gov', compress = True, skip = True):
    full_path = os.path.join(path, ticker)
    if (not os.path.exists(full_path)):
        os.makedirs(full_path)
    for rep in get_report_list(ticker):
        download_report_files(rep['url'], base_url, full_path, rep['film_num'], compress, skip)
    return 0    