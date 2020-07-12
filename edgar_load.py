from bs4 import BeautifulSoup
import requests

def parse_report_list_page(soup):
    result = []
    for tr in soup.findAll("table", {"class": "tableFile2"})[0].findAll("tr"):
        tds = tr.findChildren("td", recursiv = False)
        if (len(tds) > 0):
            if(tds[0].text in ['10-K', '10-Q']):
                result.append(tr)
    return result

def has_next_button(soup, cnt = 100):
    return len(soup.findAll("input", {"type": "button", "value": "Next "+str(cnt)})) > 0
    

def get_CIK_url(ticker, start = 0, count = 100):
    return 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=&dateb=&owner=include&start={1}&count={2}'.format(ticker, str(start), str(count))

def get_ticker_url(ticker, start = 0, count = 100):
    return 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=&dateb=&owner=include&start={1}&count={2}'.format(ticker, str(start), str(count))
    
def get_report_list(ticker):
    result = []
    has_next = True
    start = 0
    count = 100
    while has_next:        
        url = get_ticker_url(ticker, start, count)
        soup = BeautifulSoup(requests.get(url).content, 'html.parser')
        lst = parse_report_list_page(soup)                
        if (len(lst)>0):
            result.append(lst)
        has_next = has_next_button(soup, count)
        start = start + count            
    return result