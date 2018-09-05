from __future__ import unicode_literals, print_function
import sys
import codecs
import json
from multiprocessing import Pool
from biotmreaders.pubtator import PubtatorReader
from alignment import align
from pymongo import MongoClient
import glog
import itertools


def iter_pair(doc_iter):
    read_doc = 0
    ori_doc_not_found = 0

    for doc in doc_iter:
        read_doc += 1
        ori_doc = db.find_one(
            {'docId': doc['docId']}, 
            {'_id':0, 'entity': 0}
        )
        if ori_doc is None:
            ori_doc_not_found += 1
            glog.info('PMID not found in mongodb: ' + doc['docId'])
            continue
        yield doc, ori_doc

    glog.info('Total PMIDs: {}'.format(read_doc))
    glog.info('Total not found PMIDs: {}'.format(ori_doc_not_found))


def align_entity(pair):
    try:
        tm_doc, ori_doc = pair
        # Use a list for mongodb. It can't index dynamic keys.
        # Only keep non-pubtator entities.
        tm_entities = tm_doc['entity'].values()

        score = align.align_entity(ori_doc['text'], tm_doc['text'], tm_entities)

        # Add sentence index.
        if 'sentence' in ori_doc:
            for entity in tm_entities:
                for sentence in ori_doc['sentence']:
                    if 'charStart' not in sentence:
                        sentence['charStart'] = 0
                    if 'index' not in sentence:
                        sentence['index'] = 0
                    # Protobuf default values.
                    index = sentence.get('index', 0)
                    char_start = sentence.get('charStart', 0)
                    char_end = sentence.get('charEnd', 0)

                    if (entity['charStart'] >= char_start and 
                        entity['charEnd'] <= char_end):
                        entity['sentenceIndex'] = index
                        break

        # Make a dictionary from duid to entity.
        ori_doc['entity'] = {t['duid']: t for t in tm_entities}
        
        # Keep all the fields from text-mining results.
        for key, value in tm_doc.items():
            if key not in ['docid', 'entity', 'sentence', 'text', 'title']:
                ori_doc[key] = value

        # Remove sentence info.
        # del ori_doc['sentence']
        
        return ori_doc, tm_doc, score
    except Exception as e:
        glog.info('Alignment error: ' + tm_doc['docId'])
        return None, None, None


def iter_pubtator(filepath):
    pmids = set()
    #with open('2017medline/updates/normal_pmids.txt') as f:
    #    #with open('pmids/phospho_pmids.txt') as f:
    #    for line in f:
    #        pmids.add(line.strip())

    reader = PubtatorReader()
    doc_iter = reader.doc_iter_file(filepath)
    return doc_iter


def iter_json_file(filepath):
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            yield json.loads(line)


if __name__ == '__main__':
    glog.setLevel(glog.WARNING)
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    client = MongoClient('localhost')
    if 'pmc' in input_file:
        db = client.pmc.text
    elif 'medline' in input_file:
        db = client.medline2018.text
    else:
        raise ValueError

    if input_file.endswith('.json'):
        doc_iter = iter_json_file(input_file)
    else:
        doc_iter = iter_pubtator(input_file)

    pair_iter = iter_pair(doc_iter)

    output = codecs.open(output_file, 'w', 'utf8')

    pool = Pool(10)
    count = 0

    while True:
        result = pool.imap_unordered(
            align_entity, itertools.islice(pair_iter, 200))

        if not result:
            break
    
        has_result = False
        for ori_doc, pub_doc, score in result:
            has_result = True
            count += 1

            if ori_doc is None:
                continue

            # MIRTEX uses 4, RLIMS-P uses 3.
            if score < 3:
                #glog.warning('Alignment score < 3: {}\n{}\n{}'.format(
                #    ori_doc['docId'], ori_doc['text'], pub_doc['text']))            
                continue

            json_line = json.dumps(ori_doc)
            output.write(json_line + '\n')

            if count % 1000 == 0:
                print(count, end='\r')
                sys.stdout.flush()

        if not has_result:
            break

    output.close()

