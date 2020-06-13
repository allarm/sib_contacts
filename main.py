import requests
import json
import urllib.parse
from pprint import pprint
from pathlib import Path
import csv

attributes={
    'LASTNAME':'',
    'FIRSTNAME':'',
    'SMS':'',
    'CITY':'',
    'COUNTRY':'',
}

def update_from_all_csv(api_key, update=False):
    """
    Getting list of CSVs and processing them
    """
    cwd = Path.cwd()
    relative_path = "../in/tables/"
    working_path = (cwd / relative_path).resolve()

    print(f"Working path: {working_path}")

    csvs = working_path.glob("*.csv")

    print("CSVs:")
    for csv in csvs:
        print(f"--{csv.name}")
        update_from_csv(api_key=api_key, filename=csv, update=update)

    return 0

def update_from_csv(api_key, filename, update=False):
    """
    Processing a csv file
    """

    print(f"CSV: {filename.name}")

    with open(filename, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names: {", ".join(row)}')
                line_count += 1

            email = row["EMAIL"]

            attributes = {
                'LASTNAME': row["LASTNAME"],
                'FIRSTNAME': row["FIRSTNAME"],
                'CITY': row["CITY"],
                'COUNTRY': row["COUNTRY"],
            }

            print(f'email: {email}, ', end='')
            print(f'first name: {attributes["FIRSTNAME"]}, ', end='')
            print(f'last name: {attributes["LASTNAME"]}, ', end='')
            print(f'city: {attributes["CITY"]}, ', end='')
            print(f'country: {attributes["COUNTRY"]}')

            response = sib_update_contact(api_key=api_key,
                                          email=email,
                                          attributes=attributes,
                                          update=update)

            print(f"Status: {response.status_code}, text: {response.text}")

            line_count += 1

    return 0
    
def get_config():
    cwd = Path.cwd()
    filename = (cwd / "../config.json").resolve()
    with open(filename) as f:
        config_json = json.load(f)
        return config_json

def parse_config(config_json):
   api_key = config_json["parameters"]["credentials"]["api_key"] 
   update_contacts = config_json["parameters"]["sb_settings"]["update_contacts"]

   return api_key, update_contacts
    
def sib_del_contact(api_key, email):
    url = "https://api.sendinblue.com/v3/contacts/{}".format(urllib.parse.quote(email))

    headers = {
        'accept': "application/json",
        'api-key': api_key
        }

    response = requests.request("DELETE", url, headers=headers)   

    return(response)

def sib_get_all_contacts(api_key):
    url = "https://api.sendinblue.com/v3/contacts"

    querystring = {"limit":"50","offset":"0"}

    headers = {
        'accept': "application/json",
        'api-key': api_key
        }

    response = requests.request("GET", url, headers=headers, params=querystring)

    return(response)

def sib_update_contact(api_key, email, attributes='', update=True):
    url = "https://api.sendinblue.com/v3/contacts"
    update_str="true" if update else "false"

    if attributes:
        attributes_str=json.dumps(attributes)

    payload = f'''
        {{"updateEnabled":{update_str},
        "email":"{email}",
        "attributes": {attributes_str}}}
        '''

    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'api-key': api_key,
        }

#    print(f"Payload: {payload}")

    response = requests.request("POST", url, data=payload, headers=headers)

    return(response)

def sib_create_contact(api_key, email, attributes='', update=False):

    response=sib_update_contact(api_key,
                                attributes=attributes,
                                email=email,
                                update=update
    )

    return(response)

if __name__ == "__main__":
    print("Getting configuration...")
    config = get_config()
    api_key, update_contacts = parse_config(config)

    update_contacts = False if not update_contacts.casefold() == "True".casefold() else True

    print(f"update_contacts: {update_contacts}")

    attributes['LASTNAME']='Bobstonts'
    attributes['FIRSTNAME']='Bob'
    email="blah@dhafe.oef"

    update_from_all_csv(api_key, update=update_contacts)
    
    #response=sib_update_contact(api_key=api_key, email=email, attributes=attributes)
    #print(response.text)

    #response=sib_del_contact(api_key=api_key, email='depo@oped.org')
    #print(response.text)

    response=sib_get_all_contacts(api_key=api_key)
    pprint(json.loads(response.text))
