#!/usr/bin/env python

'''
Thumbs up/down on whether all ipfs are filled over an input AOI
'''
from __future__ import print_function
import os
import re
import json
import argparse
import urllib3
import requests
from datetime import datetime
import dateutil.parser
from hysds.celery import app

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def main(aoi_id, aoi_index, acq_index, track_number):
    '''main loop.'''
    #get aoi info
    aoi = get_aoi(aoi_id, aoi_index)
    #get local acquisitions
    print('querying es...')
    es_ids = get_es_objects(aoi, acq_index, track_number)
    print('found {} total es ids.'.format(len(es_ids)))    
    #print results
    if not es_ids:
        print('There are no missing ipfs!')
    else:
        print('Missing ipfs count: {}'.format(len(es_ids)))
        print('Acquisitions:\n{}'.format('\n'.join(es_ids)))

def get_es_objects(aoi, acq_index, track_number):
    starttime = aoi.get('_source', {}).get('starttime')
    endtime = aoi.get('_source', {}).get('endtime')
    location = aoi.get('_source', {}).get('location')
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, acq_index)
    grq_query = {"query":{"filtered":{"query":{"geo_shape":{"location": {"shape":location}}},"filter":{"bool":{"must":[{"term":{"metadata.track_number":track_number}},{"range":{"endtime":{"from":starttime}}},{"range":{"starttime":{"to":endtime}}}],"must_not":[{"term":{"metadata.tags":"deprecated"}},{"exists":{"field":"metadata.processing_version.raw"}}]}}}},"from":0,"size":1000}
    #print(json.dumps(grq_query))
    results = query_es(grq_url, grq_query)
    slc_id_list = [x.get('_id') for x in results]
    return slc_id_list

def get_aoi(aoi_id, aoi_index):
    '''
    retrieves the AOI from ES
    '''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, aoi_index)
    es_query = {"query":{"bool":{"must":[{"term":{"id.raw":aoi_id}}]}}}
    result = query_es(grq_url, es_query)
    if len(result) < 1:
        raise Exception('Found no results for AOI: {}'.format(aoi_id))
    return result[0]

def query_es(grq_url, es_query):
    '''
    Runs the query through Elasticsearch, iterates until
    all results are generated, & returns the compiled result
    '''
    # make sure the fields from & size are in the es_query
    if 'size' in es_query.keys():
        iterator_size = es_query['size']
    else:
        iterator_size = 1000
        es_query['size'] = iterator_size
    if 'from' in es_query.keys():
        from_position = es_query['from']
    else:
        from_position = 0
        es_query['from'] = from_position
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    response.raise_for_status()
    results = json.loads(response.text, encoding='ascii')
    results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    for i in range(iterator_size, total_count, iterator_size):
        es_query['from'] = i
        response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
        response.raise_for_status()
        results = json.loads(response.text, encoding='ascii')
        results_list.extend(results.get('hits', {}).get('hits', []))
    return results_list

def parser():
    '''
    Construct a parser to parse arguments
    @return argparse parser
    '''
    parse = argparse.ArgumentParser(description="Determines if all acquisitions have ipfs that are filled.")
    parse.add_argument("--aoi", help="AOI id", dest='aoi_name', required=True)
    parse.add_argument("--track", help="track number", dest='track_number', required=True)
    parse.add_argument("--aoi_index", help="AOI Index", default= "grq_*_area_of_interest", dest='aoi_index', required=False)
    parse.add_argument("---acq_index", help="Acquisition index", default="grq_*_acquisition-s1-iw_slc", dest="acq_index", required=False)
    return parse

if __name__ == '__main__':
    args = parser().parse_args()
    main(args.aoi_name, args.aoi_index, args.acq_index, int(args.track_number))
