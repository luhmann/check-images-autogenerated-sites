from __future__ import print_function
import requests
import pickle
import os

baseDir = os.path.dirname(os.path.realpath(__file__))
pickleFile = os.path.join(baseDir, 'missing.txt')
images = pickle.load(open(pickleFile, 'rb'))

for img in images:
    print('-----------')
    print('Checking %s' % img)
    r = requests.get('http://10.228.39.112:8098/riak/ez/' + img)

    if r.status_code != 404:
        print ('Image %s seems to exist' % img)