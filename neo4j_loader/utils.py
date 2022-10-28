#! /usr/bin/env python3
""" Common utility functions. """

import os
from math import floor, log10
import re
import html

root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
neo4j_import_dir = os.path.join(os.getenv("NEO4J_HOME"), "import")

REVIEW_COUNT = 233055327
META_COUNT = 15454958

TAG_CLEANER = re.compile('<.*?>')
SPECIAL_CHARACTER_CHECKER = re.compile("[^a-zA-Z0-9]+$")
SPECIAL_CHARACTERS = re.compile('[^a-zA-Z0-9]+')
SPECIAL_CHARACTERS_EXCEPT_DOT = re.compile('[^a-zA-Z0-9.]+')

BRAND_REPLACE_PATTERNS = [("Visit Amazon's", "Page"), ("(", ")"), ("\"", "\""),
                          ("{", "}")]


def clean_html(raw_html):
    """
    Remove HTML tags and unescape.
    """
    cleantext = html.unescape(raw_html)
    cleantext = re.sub(TAG_CLEANER, '', cleantext)
    return cleantext.replace('\n', '').strip()


def output_word_frequency(freq, filename):
    """
    Output word frequency.
    """
    freq_path = os.path.join(root, "neo4j_loader", filename)
    with open(freq_path, "w") as outf:
        for word, count in sorted(freq.items(),
                                  key=lambda x: x[1],
                                  reverse=True):
            outf.write(f"{word} {count}\n")
    print(f"output word frequency to {freq_path}")


def escape_comma_quote(value):
    """ Escape comma for CSV with quote char. """
    if '"' in value:
        value = value.replace('\\"', '"').replace('""', '"')
        value = value.replace('"', '""')
        return f'"{value}"'
    if "," in value:
        return f'"{value}"'
    return value


def escape_comma_newline(value):
    """ Escape comma and newline for CSV with quote char. """
    if len(value) == 0:
        return value
    value = value.replace('\n', '\\n')
    value = value.replace('\r\n', '\\n')
    value = value.replace('\r', '\\n')

    return escape_comma_quote(value)


def output_node_file(distinct, label, col='name'):
    """
    Output node file following NEO4J CSV format
    """
    n_digits = floor(log10(len(distinct))) + 1
    output_path = os.path.join(neo4j_import_dir, f"{label}.csv")
    with open(output_path, "w") as outf:
        outf.write(f"id:ID({label}_id),{col}:string\n")
        if isinstance(distinct, dict):
            distinct = distinct.values()
        for idx, value in enumerate(sorted(distinct)):
            idx = str(idx).zfill(n_digits)
            value = escape_comma_quote(value)
            outf.write(f"{idx},{value}\n")
    print(f"output to {output_path}")


def simplify_value(value):
    """ Removes special chars and return case-insensitive signature """
    signature = re.sub(SPECIAL_CHARACTERS, ' ', value.lower()).strip()
    simplified_value = re.sub(SPECIAL_CHARACTERS_EXCEPT_DOT, ' ',
                              value).strip()
    return signature, simplified_value


def clean_brand_values(value, replace):
    """
    @returns The cleaned value if valid, else None
    """
    value = clean_html(value)
    # replace irrelevant words
    if replace is not None:
        for rule in replace:
            if isinstance(rule, tuple):
                start, end = rule
                if value.startswith(start) and value.endswith(end):
                    value = value[len(start):len(value) - len(end)].strip()
            else:
                if rule in value:
                    value = value.replace(rule, "")
            if len(value) == 0:
                return None
    # skip brand names that contain only special characters
    if SPECIAL_CHARACTER_CHECKER.match(value):
        return None
    return value.strip()


def clean_style_key(key):
    """ Removes the trailing colon and apply title format. """
    if key.endswith(':'):
        key = key[:-1]
    return key.title()
