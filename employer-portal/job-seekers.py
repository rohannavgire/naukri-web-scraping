from bs4 import BeautifulSoup as bs
import requests

# https://enterprise.naukri.com/recruit/login?msg=TO&URL=https%3A%2F%2Fresdex.naukri.com%2Fv2%2Fsearch%2FpageChange%3FSRCHTYPE%3Dadv%26sid%3D4875578459
URL = 'https://enterprise.naukri.com/'
LOGIN_ROUTE = ''
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36', 'origin': URL, 'referer': URL + LOGIN_ROUTE}

s = requests.session()

# csrf_token = s.get(URL).cookies['csrf_token']
# print(s.get(URL).text)

login_payload = {
    'login': 'ashwini.bogadi@worldemp.com',
    'password': 'Hello@123'
}

login_req = s.post(URL + LOGIN_ROUTE, headers=HEADERS, data=login_payload)
# print(login_req)

cookies = login_req.cookies

# https://resdex.naukri.com/v2/search/pageChange?SRCHTYPE=adv&sid=4875701897

soup = bs(s.get('https://enterprise.naukri.com/recruit/login?msg=TO&URL=https%3A%2F%2Fresdex.naukri.com%2Fv2%2Fsearch%2FpageChange%3FSRCHTYPE%3Dadv%26sid%3D4875578459').text, 'html.parser')
results = soup.find(class_='txt')
print(soup)
# job_elems = results.find_all('span',class_='jobTuple bgWhite br4 mb-8')