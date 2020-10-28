'''
Determines the number of gunws generated over an AOI for a given time range.
'''
import argparse
from collections import defaultdict
from elasticsearch import Elasticsearch
import datetime
from dateutil.relativedelta import relativedelta
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def connect_to_host():
    grq = Elasticsearch(GRQ_URL, verify_certs=False)
    if not grq.ping():
        return 1
    return grq

def validateAOIs(aois):
    aoi_list = []
    if aois != "all":
        aoi_list = aois.split(",")
        for aoi in aoi_list:
            if not grq.exists(index="grq_v3.0_area_of_interest", doc_type="area_of_interest", id=aoi):
                print("WARNING: %s does not exist. Skipping..." % aoi)
                aoi_list.remove(aoi)
    else: # pull all aoi id's from grq
        print("Querying over all AOI's...")
        res = grq.search(index="grq_v3.0_area_of_interest", doc_type="area_of_interest", size=1000)
        for doc in res['hits']['hits']:
            aoi_list.append(doc["_id"])
    return aoi_list

def getTimeRange(time):
    if 'd' in time: # day
        d = int(time.split("d")[0])
        tmp = datetime.date.today() - datetime.timedelta(d)
    elif 'm' in time: # month
        m = int(time.split("m")[0])
        tmp = datetime.date.today() - relativedelta(months=+m)
    else: # since the beginning of S1 collection
        tmp = "2014-01-01"
    start_time = str(tmp) + "T00:00:00"
    return start_time

def getGUNWCounts(aoi_list, start_time):
    # look at gunws over time range and if they have the proper aoi, store id and get tags
    # after each aoi is complete, print results
    tags = defaultdict(list)
    aoi_query = ""
    for aoi in aoi_list:
        aoi_query = aoi_query + aoi + " OR "
    aoi_query = aoi_query[:-4]
    doc = {"query":{"bool":{"must":[{"range":{"creation_timestamp":{"gt":start_time,"lt":"now"}}},{"query_string":{"default_field":"metadata.tags.raw","query": aoi_query}}]}},"from":0,"sort":[],"aggs":{}}
    res = grq.search(index="grq_v2.0.3_s1-gunw", body=doc)
    # for each result, look at tags, create dict of tag: gunwid
    for hit in res['hits']['hits']:
        tag_list = hit["_source"]["metadata"]["tags"]
        gunw_id = hit["_id"]
        for tag in tag_list:
            tags[tag].append(gunw_id)
    printTags(tags)
    return tags

def printTags(tags):
    total: int = 0
    if VERBOSE:
        print("******************************************************")
        print("GUNW products generated for each tag since %s" % start_time)
        print()

        for tag in tags:
            print()
            print("*****************************************************")
            print(tag + ": " + str(len(tags[tag])))
            total += int(len(tags[tag]))
            for gunw in tags[tag]:
                print(gunw)
    else:
        print("GUNW products generated for each tag since %s" % start_time)
        print()
        for tag in tags:
            print(tag + ": " + str(len(tags[tag])))
            total += int(len(tags[tag]))
    print()
    print("Total number of GUNW products for combined tags: %i" % total)

if __name__ == '__main__':
    VERBOSE = False
    parser = argparse.ArgumentParser()
    parser.add_argument('--aoi', "--aoi", default="all", help='AOIs from which to query gunws [default searches all AOIs]')
    parser.add_argument('--verbose', action='store_true', help='Prints the gunw ids. Without it, only the numbers will be printed.')
    parser.add_argument('--time', "--time", default="1d", help='Time range over which to look at to report gunw generation.')

    # Connection parameters
    #GRQ_URL = 'https://100.67.35.28/es/'
    GRQ_URL = 'http://100.67.35.28:9200'

    grq = connect_to_host()
    if grq == 1:
        print("Failed to connect to host.")
        exit(1)
    else:
        print("Connected to GRQ")

    args = parser.parse_args()
    aoi_list = validateAOIs(args.aoi)
    start_time = getTimeRange(args.time)

    if args.verbose:
        print("Verbose flag is set. GUNW ID\'s will be printed.")
        VERBOSE = True

    gunw_counts = getGUNWCounts(aoi_list, start_time)
