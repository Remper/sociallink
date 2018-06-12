import argparse, json
import gzip
from urllib import request
from urllib.parse import urlencode
from urllib.error import HTTPError

from os import path

API = "http://localhost:5241/transform/"
API_LIST = "http://localhost:5241/embeddings"


def get_list_of_embeddings() -> list:
    try:
        with request.urlopen(API_LIST) as raw:
            response = json.loads(raw.read().decode('utf-8'))["data"]
            result = []
            for emb in response:
                if emb.startswith("kb"):
                    result.append(emb)
            return result

    except HTTPError as e:
        print("Error happened while requesting the list of embeddings:", e)
        return []


def query_api(wikidata_uri: str, emb: str) -> list:
    url = API + emb + '?' + urlencode({'followees': json.dumps([wikidata_uri])})

    try:
        with request.urlopen(url) as raw:
            response = json.loads(raw.read().decode('utf-8'))["data"]
            if response["resolved"] == 0:
                print("Didn't find embedding for entity %s (req: %d, res: %d)"
                      % (wikidata_uri, response["requested"], response["resolved"]))
            return response["embedding"]

    except HTTPError as e:
        print("Error happened:", e, "Request:", wikidata_uri)
        return None


def main(gold_file, gold_filtered, output):
    filtered = set()
    with open(gold_filtered, 'r') as reader:
        for line in reader:
            twitter_id = line.rstrip().split('\t')[0].lower()
            filtered.add(twitter_id)

    print("Loaded %d entries from the filtered gold standard" % len(filtered))

    gold = dict()
    header = None
    with open(gold_file, 'r') as reader:
        for line in reader:
            if header is None:
                header = line
                continue
            row = line.rstrip().split(',')
            twitter_id = row[1].lower()
            wikidata_uri = row[0]
            if twitter_id in filtered:
                filtered.remove(twitter_id)
                gold[wikidata_uri] = twitter_id

    print("Loaded %d entries from the gold standard" % len(gold))

    for emb in get_list_of_embeddings():
        print("Embedding: %s" % emb)
        counter = 0
        with gzip.open(path.join(output, "kb."+emb+".gz"), 'wt') as writer:
            for wikidata_uri in gold:
                counter += 1
                if counter % 2000 == 0:
                    print("Processed %d entities" % counter)

                embedding = query_api(wikidata_uri, emb)
                if embedding is None or len(embedding) == 0:
                    print("Incorrect embedding for entity: %s" % embedding)
                    continue

                writer.write(gold[wikidata_uri])
                for i in range(len(embedding)):
                    writer.write(' ')
                    writer.write(str(i+1))
                    writer.write(':')
                    writer.write(str(embedding[i]))
                writer.write('\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts knowledge base embeddings from the API')
    parser.add_argument('--gold', metavar='#', help='gold standard')
    parser.add_argument('--filtered', metavar='#', help='filtered gold standard')
    parser.add_argument('--output', metavar='#', help='folder for the output')
    args = parser.parse_args()

    main(args.gold, args.filtered, args.output)
