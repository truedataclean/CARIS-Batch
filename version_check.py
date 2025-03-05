# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 11:11:33 2023

@author: PKhosla
"""

import requests
import json

def version_check(domain, layer_id, api_key):
    """Get the latest version's id of the layer"""

    url = f"https://{domain}/services/api/v1/layers/{layer_id}/versions/"
    
    payload={}
    headers = {
      'Authorization':  f"{api_key}",
      'Cookie': 'csrftoken=qx4A0OcdF99PpTlEaIbrT5NsgBMuJ2CbjJjsBhmJKs1dK41dauwA8DIchTroLWqR; sessionid=3zbxatiuivra8zp7q82n2xlaxr56yox3'
    }
    
    response = requests.request("GET", url, headers=headers, data=payload)
    
    versions = response.text
    versions = json.loads(versions)
    version = versions[0]
    version_id = version['id']
    version_url = version['url']
    return version_id, version_url
    