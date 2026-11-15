# -*- coding: utf-8 -*-

"""
Downloads all MTG cards from api.magicthegathering.io (paged, 100 cards per
request) and stores every API response unchanged as one JSON file per page:

    <target_dir>/cards_page_00001.json
    <target_dir>/cards_page_00002.json
    ...

The API is rate limited, so every request is retried with a growing pause when
the API answers with an error or signals that the limit has been reached.
"""

import argparse
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

API_URL = 'https://api.magicthegathering.io/v1/cards'
PAGE_SIZE = 100
MAX_RETRIES = 8


def get_args():
    parser = argparse.ArgumentParser(description='Download MTG cards as raw JSON pages.')
    parser.add_argument('--target_dir', help='Local directory for the JSON files, e.g. /home/airflow/mtg', required=True, type=str)
    parser.add_argument('--max_pages', help='Limit number of pages (0 = all pages)', required=False, type=int, default=0)
    parser.add_argument('--threads', help='Number of parallel downloads', required=False, type=int, default=4)
    return parser.parse_args()


def fetch_page(session, page):
    """
    Requests one page and returns the response. Retries on network errors,
    HTTP errors and rate limiting.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(API_URL, params={'page': page, 'pageSize': PAGE_SIZE}, timeout=120)
            if response.status_code == 200:
                response.json()  # make sure the body is complete, valid JSON
                return response
            wait = 60 if response.status_code in (403, 429) else 5 * attempt
            print('Page %d: HTTP %d, retry %d/%d in %ds' % (page, response.status_code, attempt, MAX_RETRIES, wait))
        except (requests.RequestException, ValueError) as error:
            wait = 5 * attempt
            print('Page %d: %s, retry %d/%d in %ds' % (page, error, attempt, MAX_RETRIES, wait))
        time.sleep(wait)
    raise RuntimeError('Page %d could not be downloaded after %d attempts' % (page, MAX_RETRIES))


def save_page(target_dir, page, response):
    """
    Writes the raw response body to disk. Returns the number of cards.
    """
    file_name = os.path.join(target_dir, 'cards_page_%05d.json' % page)
    with open(file_name + '.tmp', 'wb') as out:
        out.write(response.content)
    os.rename(file_name + '.tmp', file_name)
    return len(response.json().get('cards', []))


if __name__ == '__main__':
    args = get_args()
    os.makedirs(args.target_dir, exist_ok=True)
    session = requests.Session()

    # The first page tells us how many cards (and therefore pages) exist
    first = fetch_page(session, 1)
    total_count = int(first.headers.get('Total-Count', 0))
    total_pages = int(math.ceil(total_count / float(PAGE_SIZE)))
    if args.max_pages > 0:
        total_pages = min(total_pages, args.max_pages)
    print('API reports %d cards, downloading %d pages (rate limit remaining: %s)'
          % (total_count, total_pages, first.headers.get('Ratelimit-Remaining')))

    cards = save_page(args.target_dir, 1, first)

    def download(page):
        response = fetch_page(session, page)
        count = save_page(args.target_dir, page, response)
        if page % 50 == 0:
            print('Page %d/%d done (rate limit remaining: %s)'
                  % (page, total_pages, response.headers.get('Ratelimit-Remaining')))
        return count

    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        cards += sum(pool.map(download, range(2, total_pages + 1)))

    print('Finished: %d cards in %d files written to %s' % (cards, total_pages, args.target_dir))
    if cards == 0:
        sys.exit('No cards downloaded')
