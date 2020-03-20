#!/usr/bin/env python

'''
Validates whether all acquisitions are localized over an input AOI and track
'''
from __future__ import print_function
from builtins import map
from builtins import range
import os
import re
import json
import argparse
import urllib3
import requests
from datetime import datetime
import dateutil.parser
import shapely.wkt
from shapely.geometry import Polygon, MultiPolygon
from hysds.celery import app

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

scihub_results = dict()
es_results = dict()
latest_slcs = list()


def main(aoi_id, aoi_index, acq_index, track_number):
    '''main loop.'''
    #get aoi info
    aoi = get_aoi(aoi_id, aoi_index)    
    #get scihub acquisitions
    print('querying scihub...')
    scihub_ids = get_scihub_objects(aoi, track_number)
    print('found {} total scihub ids:'.format(len(scihub_ids)))
    print('\n'.join(scihub_ids))
    #get local acquisitions
    print('querying es...')
    es_ids = get_es_objects(aoi, acq_index, track_number)
    print('found {} total es ids:'.format(len(es_ids)))
    print('\n'.join(es_ids))
    print("sets scihub_ids: {}".format(set(scihub_ids)))
    print("sets es_ids: {}".format(set(es_ids)))
    #determine coverage differences
    diff = list(set(scihub_ids) - set(es_ids))
    missing_acqs = [scihub_results.get(i).get("slc_id") for i in diff]
    #see if intersection and if so then verify if we have the latest
    for existing in intersection(es_ids, scihub_ids):
        #compare ingestion times
        print("es_result ingestion time: {}".format(es_results.get(existing).get("ingestion_time")))
        print("scihub_result ingestion time: {}".format(scihub_results.get(existing).get("ingestion_time")))
        if dateutil.parser.parse(es_results.get(existing).get("ingestion_time")) < dateutil.parser.parse(scihub_results.get(existing).get("ingestion_time")):
            print("Outdated acquisition ID: {} \n Latest SLC Id: {}\nES Time: {}, SciHub Time: {}".format(existing, scihub_results.get(existing).get("slc_id"), es_results.get(existing).get("ingestion_time"), scihub_results.get(existing).get("ingestion_time")))
            latest_slcs.append(scihub_results.get(existing).get("slc_id"))
    print("diff: {}".format(diff))
    print("latest_slcs: {}".format(latest_slcs))
    if not diff and not latest_slcs:
        print('There are no missing acquisitions!')
    else:
        print('Missing acquisition count: {}'.format(len(diff)+len(latest_slcs)))
        print('Missing acquisitions:\n')
        for slc in diff:
            if isinstance(slc, dict):
                for key,value in slc.items():
                    print("{}:{}".format(key, value))
                    print("{} , Ingestion Date : {}".format(scihub_results.get(key).get("slc_id")), scihub_results.get(key).get("ingestion_time"))
            else:
                print("{}".format(scihub_results.get(slc).get("slc_id")))

        for slc in latest_slcs:
            print(slc)

def intersection(lst1, lst2): 
    return list(set(lst1) & set(lst2))


def get_scihub_objects(aoi, track_number):
    url = 'https://scihub.copernicus.eu/apihub/search?'
    session = requests.session()
    polygon = convert_geojson(aoi.get('_source', {}).get('location'))
    starttime = aoi.get('_source', {}).get('starttime')
    endtime = aoi.get('_source', {}).get('endtime')
    query = 'relativeorbitnumber:{0} AND IW AND producttype:SLC AND platformname:Sentinel-1 AND beginposition:[{1} TO {2}] ( footprint:"Intersects({3})")'.format(track_number, starttime, endtime, polygon)
    offset = 0
    loop = True
    total_results_expected = None
    results_list = []
    while loop:
        query_params = {"q": query, "rows": 100, "format": "json", "start": offset }
        response = session.get(url, params=query_params, verify=False)
        response.raise_for_status()
        results = response.json()
        if total_results_expected is None:
            total_results_expected = int(results['feed']['opensearch:totalResults'])
        entries = results['feed'].get('entry', None)
        #print("entries: {}".format(entries))
        if entries:
            for entry in entries:
                for date in entry.get('date'):
                    if date["name"] == "ingestiondate":
                        ingest_date = date["content"]
                    if date["name"] == "beginposition":
                        sensing_start = date["content"]
                    if date["name"] == "endposition":
                        sensing_stop = date["content"]
                for str in entry.get('str'):
                    if str["name"] == "sensoroperationalmode":
                        mode = str["content"]
                for i in entry.get('int'):
                    if i['name'] == 'relativeorbitnumber':
                        track_number = int(i['content'])

                title = entry.get('title')
                id = "acquisition-{}-esa_scihub".format(title)
             
                try:
                    if dateutil.parser.parse(scihub_results.get(id).get("ingestion_time")) < dateutil.parser.parse(ingest_date):
                        scihub_results[id] = {"slc_id": entry.get('title'), "ingestion_time": ingest_date}
                except AttributeError:
                    scihub_results[id] = {"slc_id": entry.get('title'), "ingestion_time": ingest_date}
                results_list.append(id)
        if entries is None: break
        count = len(entries)
        offset += count
    return results_list


def convert_geojson(input_geojson):
    '''Attempts to convert the input geojson into a polygon object. Returns the object.'''
    if type(input_geojson) is str:
        try:
            input_geojson = json.loads(input_geojson)
        except:
            try:
                input_geojson = ast.literal_eval(input_geojson)
            except:
                raise Exception('unable to parse input geojson string: {0}'.format(input_geojson))
    #attempt to parse the coordinates to ensure a valid geojson
    depth = lambda L: isinstance(L, list) and max(list(map(depth, L)))+1
    d = depth(input_geojson)
    try:
        # if it's a full geojson
        if d is False and 'coordinates' in list(input_geojson.keys()):
            polygon = MultiPolygon([Polygon(input_geojson['coordinates'][0])])
            return polygon
        else: # it's a list of coordinates
            polygon = MultiPolygon([Polygon(input_geojson)])
            return polygon
    except:
        raise Exception('unable to parse geojson: {0}'.format(input_geojson))


def convert_to_wkt(input_obj):
    '''converts a polygon object from shapely into a wkt string for querying'''
    return shapely.wkt.dumps(convert_geojson(input_obj))


def get_timestamp_for_filename(time):
    time = time.replace("-", "")
    time = time.replace(":", "")
    return time


def get_es_objects(aoi, acq_index, track_number):
    starttime = aoi.get('_source', {}).get('starttime')
    endtime = aoi.get('_source', {}).get('endtime')
    location = aoi.get('_source', {}).get('location')
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, acq_index)
    print("GRQ URL: {}".format(grq_url))
    grq_query = {"query":{"filtered":{"query":{"geo_shape":{"location": {"shape":location}}},
                                      "filter":{"bool":{"must":[{"term":{"metadata.track_number":track_number}},
                                                                {"range":{"endtime":{"from":starttime}}},
                                                                {"range":{"starttime":{"to":endtime}}}]}}}},"from":0,"size":1000}
    results = query_es(grq_url, grq_query)
    slc_id_list = [x.get('_id') for x in results]
    for x in results:
        slc_id_list.append(x.get("_id"))
        es_results[x.get("_id")] = {"slc_id": x.get("_source").get("metadata").get("title"),
                                    "ingestion_time": x.get("_source").get("metadata").get("ingestiondate")}
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


def get_acq(_id, _index="grq_v*_acquisition-s1-iw_slc"):
    '''
    retrieves the AOI from ES
    '''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, _index)
    es_query = {"query":{"bool":{"must":[{"term":{"_id":_id}}]}}}
    result = query_es(grq_url, es_query)
    if len(result) < 1:
        raise Exception('Found no results for AOI: {}'.format(_id))
    return result[0]


def query_es(grq_url, es_query):
    '''
    Runs the query through Elasticsearch, iterates until
    all results are generated, & returns the compiled result
    '''
    # make sure the fields from & size are in the es_query
    if 'size' in list(es_query.keys()):
        iterator_size = es_query['size']
    else:
        iterator_size = 1000
        es_query['size'] = iterator_size
    if 'from' in list(es_query.keys()):
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


def get_accurate_times(filename_str, starttime_str, endtime_str):
    '''
    Use the seconds from the start/end strings to append to the input filename timestamp to keep accuracy

    filename_str -- input S1_IW_SLC filename string
    starttime -- starttime string from SciHub metadata
    endtime -- endtime string from SciHub metadata
    '''
    match_pattern = "(?P<spacecraft>S1\w)_IW_SLC__(?P<misc>.*?)_(?P<s_year>\d{4})(?P<s_month>\d{2})(?P<s_day>\d{2})T(?P<s_hour>\d{2})(?P<s_minute>\d{2})(?P<s_seconds>\d{2})_(?P<e_year>\d{4})(?P<e_month>\d{2})(?P<e_day>\d{2})T(?P<e_hour>\d{2})(?P<e_minute>\d{2})(?P<e_seconds>\d{2})(?P<misc2>.*?)$"
    m = re.match(match_pattern, filename_str)
    start_microseconds = dateutil.parser.parse(starttime_str).strftime('.%f').rstrip('0').ljust(4,
                                                                                                '0') + 'Z'  # milliseconds + postfix from metadata
    end_microseconds = dateutil.parser.parse(endtime_str).strftime('.%f').rstrip('0').ljust(4,
                                                                                            '0') + 'Z'  # milliseconds + postfix from metadata
    starttime = "{}-{}-{}T{}:{}:{}{}".format(m.group("s_year"), m.group("s_month"), m.group("s_day"), m.group("s_hour"),
                                             m.group("s_minute"), m.group("s_seconds"), start_microseconds)
    endtime = "{}-{}-{}T{}:{}:{}{}".format(m.group("e_year"), m.group("e_month"), m.group("e_day"), m.group("e_hour"),
                                           m.group("e_minute"), m.group("e_seconds"), end_microseconds)
    return starttime, endtime


def parser():
    '''
    Construct a parser to parse arguments
    @return argparse parser
    '''
    parse = argparse.ArgumentParser(description="Determines if all acquisitions have been retrieved over an AOI")
    parse.add_argument("--aoi", help="AOI id", dest='aoi_name', required=True)
    parse.add_argument("--track", help="track number", dest='track_number', required=True)
    parse.add_argument("--aoi_index", help="AOI Index", default= "grq_*_area_of_interest", dest='aoi_index', required=False)
    parse.add_argument("---acq_index", help="Acquisition index", default="grq_*_acquisition-s1-iw_slc", dest="acq_index", required=False)
    return parse


if __name__ == '__main__':
    args = parser().parse_args()
    main(args.aoi_name, args.aoi_index, args.acq_index, int(args.track_number))
