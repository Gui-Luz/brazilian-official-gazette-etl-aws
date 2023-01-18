import json
import requests
import hashlib


def lambda_handler(event):
    try:
        body = event['Records'][0]['body']
        body_json = json.loads(body)
        full_article_string = body_json['responsePayload']
        full_article_json = json.loads(full_article_string)

        hash_id = hashlib.sha256(full_article_string.encode('utf-8')).hexdigest()
        index = 'douscraper_v1'
        endpoint = f"https://vpc-elasticdou-rgq6ubseglllogyn3ruf3q4rci.us-east-2.es.amazonaws.com/{index}/_create/{hash_id}"
        r = requests.post(endpoint, json=full_article_json)

        print({
            'message': 'Success in communicating with ElasticSearch, please review body for Elastic response.',
            'statusCode': 200,
            'body': r.json()
        })

    except Exception as e:

        print({
            'message': 'Error',
            'statusCode': 400,
            'body': e
        })

