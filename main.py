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
    results = []

    logging.info(f"Working path: {working_path}")

    csvs = working_path.glob("*.csv")

    action = config["op_action"].casefold()

    logging.info(f"Action: {action}")
    logging.info("CSVs:")

    for csv in csvs:
        logging.info(f"{csv.name}")

        if action == "update":
            results.append(
                update_from_csv(config=config, filename=csv, update=True, delete=False,)
            )
        elif action == "create":
            results.append(
                update_from_csv(
                    config=config, filename=csv, update=False, delete=False,
                )
            )
        elif action == "delete":
            results.append(
                update_from_csv(config=config, filename=csv, update=False, delete=True)
            )
        else:
            logging.error(f"Wrong action '{action}' in process_all_csvs context.")
            raise ValueError(f"Wrong action '{action}' in process_all_csvs context.")

    return results


def update_from_csv(config, filename, update=False, delete=False):
    """
    Processing a csv file
    """

    results = []

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
                    config=config, email=email, attributes=attributes, update=update,
                )
            else:
                logging.info(f"Deleting contact: {email}")

                response = sib_del_contact(config=config, email=email)

            logging.info(f"Status: {response.status_code}, text: {response.text}")

            line_count += 1

            if delete:
                action = "delete"
            elif update:
                action = "update"
            elif not update:
                action = "create"
            else:
                action = "I can't do that, Dave."

            results.append(
                {
                    "response": response,
                    "email": email,
                    "action": action,
                    "response status": response.status_code,
                    "response text": response.text,
                    "response ok": response.ok,
                }
            )

    return results


def get_config():
    """Read config file
    
    Parameters
    ----------
       no
    
    Returns
    -------
        config_json : json
    """

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
        config["listIds"] = config_json["parameters"]["listIds"]
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

    logging.info("Parsed config:")
    api_key = config["api_key"]
    config["api_key"] = "X" * len(api_key)
    logging.info(f"{pformat(config)}")
    config["api_key"] = api_key
    return config


def sib_del_contact(config, email):
    url = "https://api.sendinblue.com/v3/contacts/{}".format(urllib.parse.quote(email))

    headers = {"accept": "application/json", "api-key": config["api_key"]}

    response = requests.request("DELETE", url, headers=headers)

    return response


def sib_get_all_contacts(config):
    url = "https://api.sendinblue.com/v3/contacts"

    querystring = {"limit": "50", "offset": "0"}

    headers = {"accept": "application/json", "api-key": config["api_key"]}

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response


def sib_update_contact(config, email, attributes="", update=True):
    """Update contact

    Parameters
    ----------
    config : dict, 
        Parsed configuration file
    email : str, 
        Email to update or add
    attributes : str, optional
        attributes, default is empty string
    update : boolean, optional
        Default is True
        If True, update the contact. 
        If False - create the contact
    
    Returns
    -------
    response object
    """

    url = "https://api.sendinblue.com/v3/contacts"
    update_str = "true" if update else "false"
    method = "POST"

    if attributes:
        attributes_str = json.dumps(attributes)

    # Do not unlink contacts from list if not updating them
    if config["unlinkListIds"] and update:
        list_string = f'"unlinkListIds":{config["unlinkListIds"]},'
        url = f"{url}/{urllib.parse.quote(email)}"
        method = "PUT"
    elif config["listIds"]:
        list_string = f'"listIds":{config["listIds"]},'
    else:
        list_string = ""

    payload = f"""
        {{"updateEnabled":{update_str},
        "email":"{email}", {list_string}
        "attributes": {attributes_str}}}"""

    logging.debug("Payload:")
    logging.debug(f"{payload}")

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": config["api_key"],
    }

    response = requests.request(method, url, data=payload, headers=headers)

    return response


def do_action(config):
    action = config["op_action"].casefold()
    results = []

    if action in [
        "update",
        "create",
        "delete",
    ]:
        results = process_all_csvs(config)

        return results
    elif action in [
        "getall",
    ]:
        logging.info("Getting all contacts")
        response = sib_get_all_contacts(config)
        logging.info(pformat(json.loads(response.text)))
        results.append(
            [
                {
                    "response": response,
                    "email": "",
                    "action": action,
                    "response status": response.status_code,
                    "response text": response.text,
                    "response ok": response.ok,
                }
            ]
        )

        return results
    else:
        error = f"Can't process '{action}' - no such action implemented."
        logging.error(error)
        raise ValueError(error)


def init_logging(
    filename,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    mode="a",
    encoding=None,
    delay=False,
):
    """Initializing logging system"""
    logging.root.handlers = []
    logging.basicConfig(
        level=level,
        format=format,
        handlers=[
            logging.FileHandler(filename, mode=mode, encoding=encoding, delay=delay),
            logging.StreamHandler(),
        ],
    )


if __name__ == "__main__":
    cwd = Path.cwd()
    filename = (cwd / "../out/files/log.log").resolve()

    # Initial init to collect logs from parser
    init_logging(filename)

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

    init_logging(filename, level=level)

    logging.info(f'Log level: {config["debug_level"].upper()}')
    logging.info("Starting...")

    results = do_action(config=config)

    logging.debug(f"{pformat(results)}")

    logging.info("Summary:")
    for r in results:
        for _ in r:
            if _["action"] == "getall":
                logging.info(f"Action: getall")
                logging.info(f"{pformat(json.loads(_['response text']))}")
            else:
                logging.info(
                    f"Action: {_['action']}; Email: {_['email']}; Response OK: {_['response ok']}; Response text: {_['response text']}"
                )
