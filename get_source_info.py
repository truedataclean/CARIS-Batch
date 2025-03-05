# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 14:41:47 2023

@author: PKhosla
"""

import requests
import version_check


class LoadMetadata:
    def __init__(self, domain, layer_id, lds_page_type, ver_id, api_key):
        self.domain = domain
        self.layer_id = layer_id
        self.lds_page_type = lds_page_type
        self.ver_id = ver_id
        self.api_key = api_key

    def get_metadata_info(self):
        """API request to get the metadata info of the script"""

        layer_url = f"https://{self.domain}/services/api/v1/{self.lds_page_type}/{self.layer_id}/versions/{self.ver_id}/"
        
        # layer_url = f"https://{self.domain}/services/api/v1/layers/{self.layer_id}/versions/{self.ver_id}/"
        # Request layer details
        layer_resp = requests.get(layer_url, headers={"Authorization": f"{self.api_key}"}, timeout=10)
        print(layer_resp)
        if not layer_resp:
            print("Error in getting response")

        layer_details = layer_resp.json()
        if not layer_details:
            print("Error in getting response json")

        feature_count = layer_details["data"]['feature_count']
        if not feature_count:
            print("Error in getting feature_count")

        group = layer_details["group"]["name"]
        if not group:
            print("Error in getting group")

        source_summary = layer_details["data"]['source_summary']
        if not source_summary:
            print("Error in getting source_summary")

        layer_description = layer_details["data"]['source_summary']['descriptions'][0]
        if not layer_description:
            print("Error in getting description")

        types = layer_details["data"]['source_summary']['types'][0]
        if not types:
            print("Error in getting types")

        return feature_count, group, source_summary, layer_description, types



class SourceInfo:
    def __init__(self, domain, layer_id, lds_page_type, api_key):
        self.domain = domain
        self.layer_id = layer_id
        self.lds_page_type = lds_page_type
        self.api_key = api_key

    def get_source_info(self):
        """get the metadata of the latest version"""

        version_id, version_url = version_check.version_check(self.domain, self.layer_id, self.api_key)
        load_metadata_instance = LoadMetadata(self.domain, self.layer_id, self.lds_page_type, version_id, self.api_key)
        feature_count, group, source_summary, layer_discription, types = load_metadata_instance.get_metadata_info()
        print(group, version_id, version_url, feature_count, source_summary, layer_discription, types)
        return version_id, version_url, feature_count, source_summary, layer_discription, types


def source_check(prev_source, current_src):
    """compare the metadata of the previous and latest 
    version of the layer to be updated"""

    if prev_source == current_src:
        print("Source is same in the new and old version \n", current_src)
    elif prev_source != current_src:
        print("Source is same in the new and old version \n old source",prev_source)
        print("new source: \n", current_src)
    else:
        print("Unable to compare the sources in new and old version")
        print("old source: \n", prev_source)
        print("new source: \n", current_src)


def feature_count_check(prev_count, new_count):
    """ALert the user if the feature count difference b/w the latest and previus version is more than 10%"""

    change = 0.1*prev_count

    print('prev_count :', prev_count)
    print('new_count :', new_count)

    if prev_count > change:
        diff = prev_count - new_count
        print('diff', diff)
        if diff > change:
            print("Feature count difference between the old and updated layer is > 10000 i.e.", diff)
            return False
        else:
            print("Feature count difference between the old and updated layer is: ", diff)
            return True
    elif new_count > prev_count:
        diff = new_count - prev_count
        print('diff', diff)
        if diff > change:
            print("Feature count difference between the old and updated layer is > 10000 i.e.", diff)
            return False
        else:
            print("Feature count difference between the old and updated layer is: ", diff)
            return True
    elif prev_count == new_count:
        print('Feature count difference between the old and updated layer is same')
        return True


def check_group_name(group_input, domain, layer_id, lds_page_type, version_id, api_key):
    """to check the group name in the config file is same as the one 
    in the metadata in LDS"""
    
    load_metadata_instance = LoadMetadata(domain, layer_id, lds_page_type, version_id, api_key)
    feature_count, group, source_summary, layer_discription, types = load_metadata_instance.get_metadata_info()
    if group == group_input:
        print("Group name is: ", group)
        return True
    else:
        print("Group name entered by user is different from the group name at the source for: ", layer_id)
        return False
     
