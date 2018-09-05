from __future__ import unicode_literals, print_function
import sys
import codecs
import json
from alignment.align import align_entity
import glog


def load_json_file(filepath):
    with open(filepath) as f:
        return [json.loads(line.strip()) for line in f]


def load_origin_file(filepath):
    docs = load_json_file(filepath)
    return {doc['docId']: doc for doc in docs}


def align_entity_caller(pair):
    tm_doc, ori_doc = pair
    try:
        # Use a list for mongodb. It can't index dynamic keys.
        # Only keep non-pubtator entities.
        tm_entities = tm_doc['entity'].values()

        score = align_entity(ori_doc['text'], tm_doc['text'], tm_entities)

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


if __name__ == '__main__':
    # glog.setLevel(glog.WARNING)
    origin_file = sys.argv[1]
    result_file = sys.argv[2]
    output_file = sys.argv[3]

    origin_docs = load_origin_file(origin_file)
    result_docs = load_json_file(result_file)
    output = codecs.open(output_file, 'w', 'utf8')

    for result_doc in result_docs:
        ori_doc = origin_docs.get(result_doc['docId'], None)
        if ori_doc is None:
            glog.warning('Can not find origin doc for doc: {}'.format(result_doc['docId']))
            continue

        ori_doc, pub_doc, score = align_entity_caller((result_doc, ori_doc))
        if score is None:
            continue

        # MIRTEX uses 4, RLIMS-P uses 3.
        if score < 3:
            # glog.warning('Alignment score < 3: {}\n{}\n{}'.format(
            #    ori_doc['docId'], ori_doc['text'], pub_doc['text']))            
            continue

        json_line = json.dumps(ori_doc)
        output.write(json_line + '\n')
            
    output.close()
