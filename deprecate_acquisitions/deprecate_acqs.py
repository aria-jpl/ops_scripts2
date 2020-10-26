import logging
import elasticsearch
from hysds.celery import app

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True


logger = logging.getLogger('ipf_scrape')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

es_url = app.conf["GRQ_ES_URL"]
_index = None
_type = None
ES = elasticsearch.Elasticsearch(es_url)


def update_document(_id):
    """
    Update the ES document with new information
    :param _id: id of product delivered to ASF
    :param delivery_time: delivery time to ASF to stamp to delivered product
    :param ingest_time: ingestion time to ASF to stamp to delivered product
    :param delivery_status: status of delivery to stamp to delivered product
    :param product_tagging:
    :return:
    """
    '''

    Note: borrowed from user_tags
    @param product_id - 
    @param delivery_time - 
    '''

    new_doc = dict()
    doc = dict()
    metadata = dict()
    metadata["tags"] = "deprecated"
    doc["metadata"] = metadata
    new_doc["doc"] = doc

    ES.update(index="grq_v2.0_acquisition-s1-iw_slc", doc_type="acquisition-S1-IW_SLC", id=_id, body=new_doc)
    return


if __name__ == "__main__":
    '''
    Main program that find IPF version for acquisition
    '''
    txt = open("deprecate_acq.txt", "r")
    for acq in txt:
        acq_id = acq.strip()
        update_document(_id=acq_id)

