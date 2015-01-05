from __future__ import print_function
from riak import RiakClient
import pysolr
import pickle
import os
import json
import logging

baseDir = os.path.dirname(os.path.realpath(__file__))
pickleFile = os.path.join(baseDir, 'ids.txt')
solr = pysolr.Solr('http://vm-solrslave-02.lve-1.magic-technik.de:8080/myvideo-search/combined/', timeout=10)

riakClient = RiakClient(protocol='http', host='10.228.39.113', http_port=8098)
riakBucket = riakClient.bucket('ez')

uploadLogger = logging.getLogger('upload_logger')
uploadLogger.setLevel(logging.DEBUG)
fhu = logging.FileHandler(os.path.join(baseDir, 'missing.log'))
fhu.setLevel(logging.INFO)
uformatter = logging.Formatter('%(message)s')
fhu.setFormatter(uformatter)
uploadLogger.addHandler(fhu)


def get_mams_ids():
    mamsIds = {}
    results = solr.search('objectType:SERIES', **{
        'rows': '1000'
    })
    print (len(results))
    for result in results:
        id = result['id']
        mamsIds[id[7:]] = result

    return mamsIds

def parse_bg_img(doc):
    if doc['images'] and len(doc['images']) > 0:
        # print (doc['images'])

        for img_string in doc['images']:
            img = json.loads(img_string)
            if img['type'] == 'BACKGROUND_IMAGE':
                path = img['path']
                if path.startswith('ez/'):
                    return path[3:]
                else:
                    print('Doc %s has a background image that is not in riak %s' % (doc['id'], img['path'] ))
            else:
                print ('Doc %s has no background image in solr. Title: %s' % (doc['id'], doc['title']))

            return None
    else:
        print ('There are no images for doc %s' % doc['id'])

def img_exists(path):
    obj = riakBucket.get(path)
    return obj.exists

if not os.path.isfile(pickleFile):
    mamsIds = get_mams_ids()
    pickle.dump(mamsIds, open(pickleFile, 'wb'))
else:
    mamsIds = pickle.load(open(pickleFile, 'rb'))


missing = []
for id, doc in mamsIds.iteritems():
    print ('-------------------------')
    print ('Checking id %s' % id)
    bg_image = parse_bg_img(doc)

    print (bg_image)

    if bg_image is not None and img_exists(bg_image) is False:
        uploadLogger.info(id)
        print ('Missing Image %s for doc %s with title %s' % (bg_image, id, doc['title']))
        missing.append(bg_image)

pickle.dump(missing, open(os.path.join(baseDir, 'missing.txt'), 'wb'))


