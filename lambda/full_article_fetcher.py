import json
import urllib.parse
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import boto3

s3 = boto3.client('s3')


def lambda_handler(event):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    obj = s3.get_object(Bucket=bucket, Key=key)
    article_string = obj['Body'].read().decode()
    article = json.loads(article_string.replace("'", '"'))

    url_title = article['urlTitle']
    link = 'https://www.in.gov.br/web/dou/-/'
    url = link + url_title

    try:
        r = requests.get(url)
        html = r.text
        soup = soup = BeautifulSoup(html, 'html.parser')
        full_article = soup.find("div", class_="texto-dou")
        full_article_text = full_article.text
        article["fullText"] = full_article_text
        article["pubDate"] = str(datetime.strptime(article["pubDate"], "%d/%m/%Y").date())

        print({
            'message': 'Success in reaching DOU website.',
            'statusCode': r.status_code,
            'url': url,
            'article': article
        })

        return (json.dumps(article))

    except Exception as e:

        print({
            'message': 'Error',
            'statusCode': 400,
            'url': url,
            'article': article,
            'error': e
        })

