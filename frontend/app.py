# -*- coding: utf-8 -*-

"""
Simple search frontend for the MTG card database.
Reads from the end-user database (MySQL), processes the user input
(card name, text or artist) and displays the search results.
"""

import os

import pymysql
from flask import Flask, abort, render_template, request

app = Flask(__name__)

PAGE_SIZE = 50

# Same normalization as in the Spark job (pyspark_mtg_final_to_mysql.py),
# which fills the columns search_name and search_all.
REPLACEMENTS = [('ß', 'ss'), ('æ', 'ae'), ('œ', 'oe')]
ACCENTS = str.maketrans('àáâãäåçèéêëìíîï'
                        'ñòóôõöøùúûüýÿ',
                        'aaaaaaceeeeiiiinoooooouuuuyy')

# Which columns a search field looks at.
SEARCH_FIELDS = {
    'all': 'Name, Text oder Artist',
    'name': 'Kartenname',
    'text': 'Kartentext',
    'artist': 'Artist',
}

# Translated names in all other languages are searched in foreign_names.
FOREIGN_NAME_MATCH = 'c.id IN (SELECT f.card_id FROM foreign_names f WHERE f.name LIKE %s)'

# Per field: WHERE clause and which pattern fills each placeholder
# ("norm" = normalized input for search_name/search_all, "raw" = input as typed).
# The table collation (utf8mb4_unicode_ci) already ignores case and accents for "raw".
WHERE_BY_FIELD = {
    'name': ('(c.search_name LIKE %s OR ' + FOREIGN_NAME_MATCH + ')', ['norm', 'raw']),
    'text': ('c.text LIKE %s', ['raw']),
    'artist': ('c.artist LIKE %s', ['raw']),
    'all': ('(c.search_all LIKE %s OR ' + FOREIGN_NAME_MATCH + ')', ['norm', 'raw']),
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


def normalize(text):
    """Lower case, umlauts and accents removed: 'Weiß' and 'WEISS' both become 'weiss'."""
    text = text.lower()
    for source, target in REPLACEMENTS:
        text = text.replace(source, target)
    return text.translate(ACCENTS)


def like_pattern(query):
    """Escapes LIKE wildcards in the user input and wraps it in %...%."""
    escaped = query.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    return '%' + escaped + '%'


def data_stats(cursor):
    cursor.execute('SELECT COUNT(*) AS cards, MAX(load_date) AS load_date FROM cards')
    return cursor.fetchone()


@app.route('/')
def index():
    query = request.args.get('q', '').strip()[:100]
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
                context['stats'] = data_stats(cursor)

                if query:
                    patterns = {'raw': like_pattern(query), 'norm': like_pattern(normalize(query))}
                    where, kinds = WHERE_BY_FIELD[field]
                    parameters = [patterns[kind] for kind in kinds]

                    cursor.execute('SELECT COUNT(*) AS total FROM cards c WHERE ' + where, parameters)
                    context['total'] = cursor.fetchone()['total']
                    context['pages'] = (context['total'] + PAGE_SIZE - 1) // PAGE_SIZE

                    cursor.execute(
                        'SELECT c.id, c.multiverse_id, c.name, c.name_de, c.type, c.mana_cost, c.color_names, '
                        'c.rarity, c.set_name, c.text, c.artist, c.image_url, c.printings_count, '
                        # matching translated name in another language, e.g. French or Japanese
                        '(SELECT f.name FROM foreign_names f WHERE f.card_id = c.id AND f.name LIKE %s '
                        " AND f.name <> COALESCE(c.name_de, '') LIMIT 1) AS foreign_name "
                        'FROM cards c WHERE ' + where +
                        # cards whose own name matches come first, then cards with a picture
                        ' ORDER BY (c.search_name LIKE %s) DESC, c.has_image DESC, c.name, c.set_name LIMIT %s OFFSET %s',
                        [patterns['raw']] + parameters + [patterns['norm'], PAGE_SIZE, (page - 1) * PAGE_SIZE])
                    context['cards'] = cursor.fetchall()
        finally:
            connection.close()
    except pymysql.MySQLError as error:
        context['error'] = 'Datenbank nicht erreichbar oder noch nicht geladen: %s' % error

    return render_template('index.html', **context)


@app.route('/card/<card_id>')
def card(card_id):
    """Detail page of one printing: all attributes, translations and other printings."""
    try:
        connection = get_connection()
        try:
            with connection.cursor() as cursor:
                stats = data_stats(cursor)
                cursor.execute('SELECT * FROM cards WHERE id = %s', [card_id])
                card_row = cursor.fetchone()
                if card_row is None:
                    abort(404)
                cursor.execute('SELECT language, name FROM foreign_names WHERE card_id = %s ORDER BY language',
                               [card_id])
                translations = cursor.fetchall()
                cursor.execute('SELECT id, set_name, set_code, rarity, multiverse_id FROM cards '
                               'WHERE name = %s AND id <> %s ORDER BY set_name LIMIT 100',
                               [card_row['name'], card_id])
                printings = cursor.fetchall()
        finally:
            connection.close()
    except pymysql.MySQLError as error:
        return render_template('card.html', card=None, stats=None,
                               error='Datenbank nicht erreichbar: %s' % error), 503

    return render_template('card.html', card=card_row, translations=translations, printings=printings,
                           stats=stats, error=None)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
