from bs4 import BeautifulSoup
import requests
import psycopg2

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

def insert_sic(db, sic):
    db.execute("insert into tbl_std_ind_codes(sic, office, name) values(%s, %s, %s);", (sic['sic'], sic['office'], sic['name']))

def insert_sic_list(db, sic_list):
    for sic in sic_list:
        insert_sic(db, sic)
        db.connection.commit() 