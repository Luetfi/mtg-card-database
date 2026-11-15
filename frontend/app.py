# -*- coding: utf-8 -*-

"""
Simple search frontend for the MTG card database.
Reads from the end-user database (MySQL), processes the user input
(card name, text or artist) and displays the search results.
"""

import os

import pymysql
from flask import Flask, render_template, request

app = Flask(__name__)

PAGE_SIZE = 50

# Which columns a search field looks at. "name" also searches translated names.
SEARCH_FIELDS = {
    'all': 'Name, Text oder Artist',
    'name': 'Kartenname',
    'text': 'Kartentext',
    'artist': 'Artist',
}

FOREIGN_NAME_MATCH = 'c.id IN (SELECT f.card_id FROM foreign_names f WHERE f.name LIKE %s)'

WHERE_BY_FIELD = {
    'name': ('(c.name LIKE %s OR ' + FOREIGN_NAME_MATCH + ')', 2),
    'text': ('c.text LIKE %s', 1),
    'artist': ('c.artist LIKE %s', 1),
    'all': ('(c.name LIKE %s OR c.text LIKE %s OR c.artist LIKE %s OR ' + FOREIGN_NAME_MATCH + ')', 4),
}


def get_connection():
    return pymysql.connect(
        host=os.environ.get('MYSQL_HOST', 'mysql'),
        port=int(os.environ.get('MYSQL_PORT', '3306')),
        user=os.environ['MYSQL_USER'],
        password=os.environ['MYSQL_PASSWORD'],
        database=os.environ.get('MYSQL_DATABASE', 'mtg'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )


def like_pattern(query):
    """Escapes LIKE wildcards in the user input and wraps it in %...%."""
    escaped = query.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    return '%' + escaped + '%'


@app.route('/')
def index():
    query = request.args.get('q', '').strip()
    field = request.args.get('field', 'all')
    if field not in WHERE_BY_FIELD:
        field = 'all'
    try:
        page = max(int(request.args.get('page', '1')), 1)
    except ValueError:
        page = 1

    context = {'query': query, 'field': field, 'fields': SEARCH_FIELDS, 'page': page,
               'cards': [], 'total': 0, 'pages': 0, 'stats': None, 'error': None}

    try:
        connection = get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) AS cards, MAX(load_date) AS load_date FROM cards')
                context['stats'] = cursor.fetchone()

                if query:
                    where, parameter_count = WHERE_BY_FIELD[field]
                    parameters = [like_pattern(query)] * parameter_count

                    cursor.execute('SELECT COUNT(*) AS total FROM cards c WHERE ' + where, parameters)
                    context['total'] = cursor.fetchone()['total']
                    context['pages'] = (context['total'] + PAGE_SIZE - 1) // PAGE_SIZE

                    cursor.execute(
                        'SELECT c.id, c.multiverse_id, c.name, c.type, c.mana_cost, c.rarity, c.set_name, '
                        'c.text, c.artist, c.image_url, c.printings_count, '
                        '(SELECT f.name FROM foreign_names f WHERE f.card_id = c.id AND f.name LIKE %s LIMIT 1) '
                        'AS foreign_name '
                        'FROM cards c WHERE ' + where +
                        # cards whose own name matches come first, then cards with a picture
                        ' ORDER BY (c.name LIKE %s) DESC, c.has_image DESC, c.name, c.set_name LIMIT %s OFFSET %s',
                        [like_pattern(query)] + parameters + [like_pattern(query), PAGE_SIZE, (page - 1) * PAGE_SIZE])
                    context['cards'] = cursor.fetchall()
        finally:
            connection.close()
    except pymysql.MySQLError as error:
        context['error'] = 'Datenbank nicht erreichbar oder noch nicht geladen: %s' % error

    return render_template('index.html', **context)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
