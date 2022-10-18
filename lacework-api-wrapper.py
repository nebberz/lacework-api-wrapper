#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Lacework API Wrapper

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pandas as pd
from datetime import datetime, timedelta
import base64
from dotenv import dotenv_values
import ast


def getCompany():
    envs = dotenv_values('.env')
    for env in envs:
        if 'COMPANY' in env:
            company = envs[env]
    return company


def getEnvs():
    getEnvs = dotenv_values('.env')
    envs = {}
    for env in getEnvs:
        if '#' not in env:
            if 'LW' in env:
                addEnv = getEnvs[env]
                convert = ast.literal_eval(addEnv)
                for c in convert:
                    envs[c] = convert[c]
    return envs


def getToken(key, company):
    url = f'https://{company}.lacework.net/api/v2/access/tokens'
    headers = {'X-LW-UAKS': key[0], 'Content-Type': 'application/json', 'Accept': 'application/json'}
    data = {'keyId': key[1], 'expiryTime': 3600}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    token = response.json()['token']
    return token


def sendQuery(token, url, filters):
    headers = {'Authorization': token, 'Content-Type': 'application/json', 'Accept': 'application/json'}
    data = json.dumps(filters)
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        r = response.json()
        return r
    else:
        return None


def paginationQuery(token, url):
    headers = {'Authorization': token, 'Content-Type': 'application/json', 'Accept': 'application/json'}
    response = requests.get(url, headers=headers)
    # print(response.text)
    if response.status_code == 200:
        r = response.json()
        return r
    else:
        print(response.text)
        return None


def decodePagination(link, url):
    links = []
    coded = link.split('/')[-1]
    decoded = base64.b64decode(coded).decode('utf-8')
    uid = decoded.split(',')[0]
    curRows = int(decoded.split(',')[1])
    maxRows = int(decoded.split(',')[2])
    while curRows <= maxRows:
        encoded = base64.b64encode('{uid},{curRows},{maxRows},0'.format(uid=uid, curRows=curRows, maxRows=maxRows).encode('ASCII')).decode('ASCII')
        returnLink = url.replace(url.split('/')[-1], f'{encoded}')
        links.append(returnLink)
        curRows += 5000
    return links


def getReport(env, envKey, company, url, filters):
    results = []
    print(f'Getting Bearer Token for {env}:')
    token = getToken(envKey, company)
    print(f'Submitting request for Lacework Data for {env}:')
    data = sendQuery(token, url, filters)
    # with open(f'{env}-data-output.json', 'w', encoding='utf-8') as f:
    # 	json.dump(data, f, ensure_ascii=False, indent=4)
    if data is not None:
        if data['paging']['urls']['nextPage']:
            print('Pagination Detected: ')
            try:
                nextPage = data['paging']['urls']['nextPage']
                print('First record obtained: ')
                results.append(pd.json_normalize(data, record_path='data'))
                print('Decoding future pagination links: ')
                links = decodePagination(nextPage, url)
                processes = []
                with ThreadPoolExecutor(max_workers=25) as executor:
                    print(f'Launching pagination threads for {env} - Please wait:')
                    for link in links:
                        processes.append(executor.submit(paginationQuery, token, link))
                num = 0
                totRows = data['paging']['totalRows'] // 5000
                for task in as_completed(processes):
                    num += 1
                    print(f'{num:03d} Threads returned of {totRows:03d}', end='\r')
                    results.append(pd.json_normalize(task.result(), record_path='data'))
                print('Retrieval complete - Merging all the outputs: ')
                final = pd.concat(results, ignore_index=True)
            except Exception as e:
                print(f'Error on {env}: {e}')
        else:
            print('No Pagination: ')
            final = pd.json_normalize(data, record_path='data')
            final.to_csv(f'{env}_output.csv', index=False)
        print(f'Environment {env} complete:')
    else:
        print(f'No data returned for {env}')


def main():
    company = getCompany()
    baseurl = f'https://{company}.lacework.net'
    # Set your API endpoint. See https://sprinklr.lacework.net/api/v2/docs#tag/OVERVIEW
    endpoint = '/api/v2/Inventory/search'
    url = baseurl + endpoint
    # Set your filters for the API call, these are specific to the endpoint you are hitting. See https://sprinklr.lacework.net/api/v2/docs#tag/OVERVIEW
    filters = {
        'timeFilter': {
            'startTime': (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d'),  # T%H:%M:%SZ time format, mixed results...
            'endTime': (datetime.now()).strftime('%Y-%m-%d')  # T%H:%M:%SZ time formaat, mixed results...
        },
        "csp": "AWS"
        # 'filters': [{'expression': 'in', 'field': 'Severity', 'values': ['Critical']}],
        # 'filters': [{'expression': 'eq', 'field': 'csp', 'value': 'aws'}],
    }
    print('--- Threaded Lacework API Wrapper ---')
    envs = getEnvs()
    for env in envs:
        getReport(env, envs[env], company, url, filters)
    print('All done. Have a nice day')


if __name__ == '__main__':
    main()
