import requests
import json
import urllib.parse
from pprint import pformat
from pathlib import Path
import csv
import logging

def process_all_csvs(config):
    """
    Getting list of CSVs and processing them
    """
    cwd = Path.cwd()
    relative_path = "../in/tables/"
    working_path = (cwd / relative_path).resolve()
    responses = []

    logging.info(f"Working path: {working_path}")

    csvs = working_path.glob("*.csv")

    action = config['op_action']
    api_key = config['api_key']
    action = config['op_action'].casefold()

    logging.info(f"Action: {action}")
    logging.info("CSVs:")

    for csv in csvs:
        logging.info(f"{csv.name}")

        if action == 'update':
            update = True
            responses.append(update_from_csv(api_key=api_key,
                                             filename=csv,
                                             update=True))
        elif action == 'create':
            update = False
            responses.append(update_from_csv(api_key=api_key,
                                             filename=csv,
                                             update=False))
        elif action == 'delete':
            logging.warning(f"Delete is not implemented yet")
            pass
        else:
            logging.error(f"Wrong action '{action}' in process_all_csvs context.")
            raise ValueError(f"Wrong action '{action}' in process_all_csvs context.")
        
    return responses

def update_from_csv(api_key, filename, update=False):
    """
    Processing a csv file
    """

    logging.info(f"CSV: {filename.name}")

    with open(filename, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.warning(f'Column names: {", ".join(row)}')
                line_count += 1

            email = row["EMAIL"]

            attributes = {
                'LASTNAME': row["LASTNAME"] if "LASTNAME" in row.keys() else "",
                'FIRSTNAME': row["FIRSTNAME"] if "FIRSTNAME" in row.keys() else "",
                'CITY': row["CITY"] if "CITY" in row.keys() else "",
                'COUNTRY': row["COUNTRY"] if "COUNTRY" in row.keys() else "",
            }

            logging.debug(f'''email: {email}, 
            first name: {attributes["FIRSTNAME"]}, 
            last name: {attributes["LASTNAME"]},
            city: {attributes["CITY"]}, 
            country: {attributes["COUNTRY"]}''')

            response = sib_update_contact(api_key=api_key,
                                          email=email,
                                          attributes=attributes,
                                          update=update)

            logging.info(f"Status: {response.status_code}, text: {response.text}")

            line_count += 1

    return response
    
def get_config():
    cwd = Path.cwd()
    filename = (cwd / "../config.json").resolve()
    with open(filename) as f:
        config_json = json.load(f)
        return config_json

def parse_config(config_json):
    config = {}
    
    config['api_key'] = config_json["parameters"]["credentials"]["api_key"] 
    config['debug_level'] = config_json["parameters"]["debug"]["level"]
    config['op_action'] = config_json["parameters"]["op"]["action"]

    return config
    
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

    response = requests.request("POST", url, data=payload, headers=headers)

    return(response)

def sib_create_contact(api_key, email, attributes='', update=False):

    response=sib_update_contact(api_key,
                                attributes=attributes,
                                email=email,
                                update=update
    )

    return(response)

def do_action(config):
    action = config['op_action'].casefold()
    responses = []
    
    if action in [
            "update",
            "create",
            "delete",
            ]:
        responses = process_all_csvs(config)

        return responses
    elif action in [
            "getall",
            ]:
        logging.info("Getting all contacts")
        response = sib_get_all_contacts(config['api_key'])
        logging.info(pformat(json.loads(response.text)))
        responses.append(response)

        return responses
    else:
        error = f"Can't process '{action}' - no such action implemented."
        logging.error(error)
        raise ValueError(error)

if __name__ == "__main__":
    cwd = Path.cwd()
    filename = (cwd / "../out/files/log.log").resolve()

    config_json = get_config()
    config = parse_config(config_json)

    logging.basicConfig(filename=filename,
                        level=config['debug_level'].upper(),
                        format='%(asctime)s %(levelname)s: %(message)s')

    logging.info("Starting...")

    do_action(config=config)
