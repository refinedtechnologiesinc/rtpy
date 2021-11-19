# %%
import requests
from bs4 import BeautifulSoup
import xmltodict
# %% Connect to Expensewire API

# SOAP request URL
url = "https://sandbox.expensewire.com/openaccess/webmethods/expensewire.asmx"

# headers
headers = {"content-type": "text/xml"}

# structured XML
payload = """<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <OpenDataConnection xmlns="http://ExpenseWire.com/">
                    <UserID>ppope@r-t-i.com</UserID>
                    <Password>qBWgII55B$uw</Password>
                    <CompanyKey>72A41A27-B38D-4319-BF3B-9D622A67A801</CompanyKey>
                </OpenDataConnection>
            </soap:Body>
        </soap:Envelope>
        """

# POST request
response = requests.request("POST", url, headers=headers, data=payload)

# prints the response
bs = BeautifulSoup(response.text, 'xml')
print(bs.prettify())
print(response.status_code)
resp_xml = list(
    bs.OpenDataConnectionResponse.OpenDataConnectionResult.children)[0]
resp_dict = xmltodict.parse(resp_xml)
session_key = resp_dict['ExpenseWire']['OpenDataConnection']['SessionKey']
user_key = resp_dict['ExpenseWire']['OpenDataConnection']['UserKey']

# %% Pull data from expense wire api

# SOAP request URL
url = "https://sandbox.expensewire.com/openaccess/webmethods/expensewire.asmx"

# headers
headers = {"content-type": "text/xml",
           "SOAPAction": "http://ExpenseWire.com/SendDataTransaction"}

# structured XML
payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <SendDataTransaction xmlns="http://ExpenseWire.com/">
                <sessionKey>{session_key}</sessionKey>
                <xmlRequest>
                    <ExpenseWire>
                        <DataTransaction Version="1.1">
                            <GLAccount>
                                <Search MaxRows="5000">
                                    <GLCode Comparison="notequal">
                                        30406
                                    </GLCode>
                                </Search>
                            </GLAccount>
                        </DataTransaction>
                    </ExpenseWire>
                </xmlRequest>
            </SendDataTransaction>
        </soap:Body>
        </soap:Envelope>
        """

# POST request
response = requests.request("POST", url, headers=headers, data=payload)

# prints the response
bs = BeautifulSoup(response.text, 'xml')
print(bs.prettify())
print(response.status_code)

# %%
