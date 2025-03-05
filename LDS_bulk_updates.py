# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 15:09:56 2022

@author: PKhosla

"""

import argparse
import json
import logging
import os
import re
import sys
import requests
import yaml
import get_source_info
from get_source_info import SourceInfo
try:
    import log
except ImportError:
    from . import log

logger = logging.getLogger(__name__)


class ConfigReader:  # pylint: disable=too-few-public-methods
    """
    Create a config object that can
    be passed to the scripts methods
    """

    def __init__(self, cwd=None):
        # READ CONFIG

        # get config path
        if not cwd:
            cwd = os.getcwd()
            print(cwd)
            regex = re.compile(r"(\\|\/)(chart_exp)$")
            print(regex)
            cwd = regex.sub("", cwd)
            print(cwd)
            cwd = os.path.join(os.sep, cwd, "configlds.yaml")
            print(cwd)

        # check config exists
        if not os.path.exists(cwd): 
            raise FileNotFoundError("Can not find config file")

        with open(cwd, "r", encoding="UTF-8") as file:
            config = yaml.load(file, Loader=yaml.Loader)

        # CONNECTION
        if "Connection" in config:
            if os.getenv("LDS_APIKEY", None):
                self.api_key = os.environ["LDS_APIKEY"]
                print(self.api_key)
            else:
                self.api_key = config["Connection"]["Api_key"]
            if not self.api_key:
                raise SystemExit("No LDS API Key Provided")
            self.domain = config["Connection"]["Domain"]
        else:
            raise SystemExit('CONFIG ERROR: No "Connection" section')

        # DATA TO PROCESS
        if "Datasets" in config:
            self.layers = config["Datasets"]["Layers"]
        else:
            raise SystemExit('CONFIG ERROR: No "Datasets" section')
            
        # Group name to which layer belong
        if "Groups" in config:
            self.group = config["Groups"]["group"]
        else:
            raise SystemExit('CONFIG ERROR: No "group" section')
            
        # Page type in LDS
        if "lds_page_type" in config:
            self.lds_page_type = config["lds_page_type"]
        else:
            raise SystemExit('CONFIG ERROR: No "lds_page_type" section')


def iterate_selective(layers):
    """
    Iterate through user supplied (via config.yaml)
    dataset IDs. Returns a generator of layer ids to process
    """

    for layer_id in layers:
        yield layer_id


def parse_args(args):
    """
    To Parse command line arguments.
    """
    cli_parser = argparse.ArgumentParser(args)
    cli_parser.add_argument("--config_file", default=None, nargs="?", help="Path to config file")
    return cli_parser.parse_args()


def get_draft_id(layer_id, api_key, domain):
    """
    To create a new draft version, a POST to the endpoint give a response
    which contains a draft_id.
    """
    draft_id_url = f"https://{domain}/services/api/v1.x/layers/{layer_id}/versions/?page_size=1"
    print('draft_id url: ' ,draft_id_url)
    payload = ""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"{api_key}",

    }

    response = requests.request("POST", draft_id_url, headers=headers, data=payload)
    response = requests.request("GET", draft_id_url, headers=headers, data=payload)

    if response.status_code != 200:
        sys.exit(f"{response} Bad request: Check Key or URL")
    else:
        response_dict = json.loads(response.text)
        draft_id = response_dict[0].get("id")
        status = response_dict[0].get("status")
        print("Draft ID: ",draft_id)
        print("Status: ",status)
        return draft_id, status



def trigger_import(layer_id, draft_id, api_key, domain):
    """
    To trigger the import from the underlying data source
    using the newly created draft ID.
    """

    import_url = f"https://{domain}/services/api/v1.x/layers/{layer_id}/versions/{draft_id}/import/"
    print('import url: ' ,import_url)
    payload = ""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"{api_key}",
    }

    response = requests.request("POST", import_url, headers=headers, data=payload)
    print(response)
    if response.status_code != 202:
        sys.exit(f"{response} Bad request: Check the draft_id")
    else:
        print("Import Triggered")
        return True


def publish_layer(layer_id, draft_id, api_key, domain):
    """
    To publish the newly imported data to LDS.
    """
    publish_url = f"https://{domain}/services/api/v1.x/layers/{layer_id}/versions/{draft_id}/publish/"

    payload = {}
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"{api_key}",
    }

    response = requests.request("POST", publish_url, headers=headers, data=payload)
    if response.status_code != 201:
        print("Failed to make an Update")
        sys.exit(f"{response} Bad request: Check the URL")
    else:
        print("data updated successfully")


def main():
    """
    Script for updating LDS layers. Written for the purpose
    of applying the updates to the multiple layersin one go.
    """

    cli_parser = parse_args(sys.argv[1:])
    config_file = cli_parser.config_file

    # CONFIG LOGGING
    log.conf_logging("root")

    # CHECK PYTHON VERSION
    if sys.version_info < (3, 3):
        raise SystemExit("Error, Python interpreter must be 3.3 or higher")

    # READ CONFIG IN
    config = ConfigReader(config_file)
    print(config.api_key)

    layer_ids = iterate_selective(config.layers)
    for layer_id in layer_ids:
        print(layer_id)
        
        source_info_instance = SourceInfo(config.domain, layer_id, config.lds_page_type, config.api_key)
        
        prev_ver_id, version_url, prev_count, o_source_summary, layer_discription, types = source_info_instance.get_source_info()
        
        valid_group = get_source_info.check_group_name(config.group, config.domain, layer_id, config.lds_page_type, prev_ver_id, config.api_key)
        
        if valid_group is True:
        
            draft_id, status = get_draft_id(layer_id, config.api_key, config.domain)
            
            if draft_id:
            
                if status == 'ok':
                
                    new_ver_id, n_version_url, new_count, n_source_summary, n_layer_discription, n_types = source_info_instance.get_source_info()
                    
                    get_source_info.source_check(o_source_summary, n_source_summary)
                    
                    feature_count = get_source_info.feature_count_check(prev_count, new_count)
                    if feature_count == True:
                        if trigger_import(layer_id, draft_id, config.api_key, config.domain) == True:
                            publish_layer(layer_id, draft_id, config.api_key, config.domain)
                    else:
                        print("There is a big difference between the feature count of the previous layer and updated layer")

                else:
                    print("Check the status of the draft created for layer: ", layer_id)
            if not draft_id:
                logger.critical("Failed to get layer %s. THIS LAYER HAS NOT BEEN PROCESSED", layer_id)
                continue
            
        else:
            print("Check the group name in Config file")


if __name__ == "__main__":
    main()
