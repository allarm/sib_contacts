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

    action = config["op_action"]
    api_key = config["api_key"]
    action = config["op_action"].casefold()

    logging.info(f"Action: {action}")
    logging.info("CSVs:")

    for csv in csvs:
        logging.info(f"{csv.name}")

        if action == "update":
            responses.append(
                update_from_csv(
                    api_key=api_key, filename=csv, update=True, delete=False,
                )
            )
        elif action == "create":
            responses.append(
                update_from_csv(
                    api_key=api_key, filename=csv, update=False, delete=False,
                )
            )
        elif action == "delete":
            responses.append(
                update_from_csv(
                    api_key=api_key, filename=csv, update=False, delete=True
                )
            )
        else:
            logging.error(f"Wrong action '{action}' in process_all_csvs context.")
            raise ValueError(f"Wrong action '{action}' in process_all_csvs context.")

    return responses


def update_from_csv(api_key, filename, update=False, delete=False):
    """
    Processing a csv file
    """

    logging.info(f"CSV: {filename.name}")

    with open(filename, mode="r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.warning(f'Column names: {", ".join(row)}')
                line_count += 1

            email = row["EMAIL"]

            attributes = {
                "LASTNAME": row["LASTNAME"] if "LASTNAME" in row.keys() else "",
                "FIRSTNAME": row["FIRSTNAME"] if "FIRSTNAME" in row.keys() else "",
                "CITY": row["CITY"] if "CITY" in row.keys() else "",
                "COUNTRY": row["COUNTRY"] if "COUNTRY" in row.keys() else "",
            }

            logging.debug(
                f"""email: {email}, 
            first name: {attributes["FIRSTNAME"]}, 
            last name: {attributes["LASTNAME"]},
            city: {attributes["CITY"]}, 
            country: {attributes["COUNTRY"]}"""
            )

            if not delete:
                if update:
                    logging.info(f"Updating contact: {email}")
                else:
                    logging.info(f"Creating contact: {email}")

                response = sib_update_contact(
                    api_key=api_key, email=email, attributes=attributes, update=update
                )
            else:
                logging.info(f"Deleting contact: {email}")

                response = sib_del_contact(api_key=api_key, email=email)

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

    try:
        config["api_key"] = config_json["parameters"]["credentials"]["api_key"]
    except:
        error = "No API key defined!"
        logging.error(error)
        raise ValueError(error)

    try:
        config["debug_level"] = config_json["parameters"]["debug"]["level"]
    except:
        error = "No debug level defined, using Info"
        logging.info(error)
        config["debug_level"] = "info"

    try:
        config["op_action"] = config_json["parameters"]["op"]["action"]
    except:
        error = "No action defined!"
        logging.error(error)
        raise ValueError(error)

    try:
        config["listIds"] = config_json["parameters"]["litIds"]
    except:
        error = "No listIds defined, using an empty list"
        logging.info(error)
        config["listIds"] = []

    try:
        config["unlinkListIds"] = config_json["parameters"]["unlinkListIds"]
    except:
        error = "No unlinkListIds defined, using an empty list"
        logging.info(error)
        config["unlinkListIds"] = []

    return config


def sib_del_contact(api_key, email):
    url = "https://api.sendinblue.com/v3/contacts/{}".format(urllib.parse.quote(email))

    headers = {"accept": "application/json", "api-key": api_key}

    response = requests.request("DELETE", url, headers=headers)

    return response


def sib_get_all_contacts(api_key):
    url = "https://api.sendinblue.com/v3/contacts"

    querystring = {"limit": "50", "offset": "0"}

    headers = {"accept": "application/json", "api-key": api_key}

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response


def sib_update_contact(api_key, email, attributes="", update=True):
    url = "https://api.sendinblue.com/v3/contacts"
    update_str = "true" if update else "false"

    if attributes:
        attributes_str = json.dumps(attributes)

    payload = f"""
        {{"updateEnabled":{update_str},
        "email":"{email}",
        "attributes": {attributes_str}}}
        """

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": api_key,
    }

    response = requests.request("POST", url, data=payload, headers=headers)

    return response


def sib_create_contact(api_key, email, attributes="", update=False):

    response = sib_update_contact(
        api_key, attributes=attributes, email=email, update=update
    )

    return response


def do_action(config):
    action = config["op_action"].casefold()
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
        response = sib_get_all_contacts(config["api_key"])
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

    if config["debug_level"].upper() == "WARNING":
        level = logging.WARNING
    elif config["debug_level"].upper() == "DEBUG":
        level = logging.DEBUG
    elif config["debug_level"].upper() == "ERROR":
        level = logging.ERROR
    elif config["debug_level"].upper() == "CRITICAL":
        level = logging.CRITICAL
    else:
        level = logging.INFO

    logging.root.handlers = []
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[logging.FileHandler(filename), logging.StreamHandler()],
    )

    logging.info("Starting...")

    do_action(config=config)
