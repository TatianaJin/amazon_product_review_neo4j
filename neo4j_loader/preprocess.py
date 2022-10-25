#! /usr/bin/env python3
""" Preprocess Amazon product review data into node and relationship files"""

import json
import os
import re
import html
from math import floor, log10
import pandas as pd
from tqdm import tqdm

root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
neo4j_import_dir = os.path.join(os.getenv("NEO4J_HOME"), "import")

REVIEW_COUNT = 233055327
META_COUNT = 15454958

TAG_CLEANER = re.compile('<.*?>')
SPECIAL_CHARACTER_CHECKER = re.compile("[^a-zA-Z0-9]+$")
SPECIAL_CHARACTERS = re.compile('[^a-zA-Z0-9]+')
SPECIAL_CHARACTERS_EXCEPT_DOT = re.compile('[^a-zA-Z0-9.]+')


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


def escape_comma(value):
    """ Escape comma for CSV with quote char. """
    value = value.replace('\\"', '"')
    if "," in value and not (value.startswith('"') and value.endswith('"')):
        value = value.replace('"', '\\"')
        value = f'"{value}"'
    return value


def output_node_file(distinct, label, col='name'):
    """
    Output node file following NEO4J CSV format
    """
    n_digits = floor(log10(len(distinct))) + 1
    output_path = os.path.join(neo4j_import_dir, "{0}.csv".format(label))
    with open(output_path, "w") as outf:
        outf.write(f"id:ID({label}_id),{col}:String\n")
        if isinstance(distinct, dict):
            distinct = distinct.values()
        for idx, value in enumerate(sorted(distinct)):
            idx = str(idx).zfill(n_digits)
            value = escape_comma(value)
            outf.write(f"{idx},{value}\n")
    print(f"output to {output_path}")


def simplify_value(value):
    """ Removes special chars and return case-insensitive signature """
    signature = re.sub(SPECIAL_CHARACTERS, ' ', value.lower())
    simplified_value = re.sub(SPECIAL_CHARACTERS_EXCEPT_DOT, ' ', value)
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


def get_brands(path,
               key="brand",
               word_frequency=True,
               replace=None,
               debug=False):
    """
    Generate Brand node file.

    @param key Brand key in JSON.
    @param word_frequency If true, calcuate word frequency in brand values.
    @param replace A list of strings or 2-tuples to remove from the raw values.
           If tuple, only remove the substrings if the string starts with the
           first element and ends with the second element.
    @param debug If true, print the original strings that gives invalid values.
    """
    distinct = dict()
    freq = dict()
    # input
    with open(path, "r") as inf:
        for linenum, line in tqdm(enumerate(inf),
                                  total=META_COUNT,
                                  desc="Line"):
            if key not in line:
                continue
            j = json.loads(line.strip())
            if key in j:
                value = clean_brand_values(j[key].strip(), replace)
                # cleaning
                if value is None:
                    if debug:
                        print(linenum, j[key])
                    continue
                # word frequency
                if word_frequency:
                    for word in value.split():
                        word = word.lower()
                        if word in freq:
                            freq[word] += 1
                        else:
                            freq[word] = 1
                # get distinct brand values
                signature, value = simplify_value(value)
                if len(value) > 0 and signature not in distinct:
                    distinct[signature] = value
    if word_frequency:
        output_word_frequency(freq, "brand_word_frequency.txt")
    # output
    output_node_file(distinct, key)


def get_categories(data_path, key="category", word_frequency=False):
    """ Generate Category node file. """
    # input
    distinct = set()
    freq = dict()
    with open(data_path, "r") as inf:
        for line in tqdm(inf, total=META_COUNT, desc="Line"):
            j = json.loads(line.strip())
            if key in j:
                # get the first category
                word = j[key]
                word = word[0]
                if "," in word:
                    word = word.replace(",", " &")
                # update distinct values
                distinct.add(word)
                # word frequency
                if word_frequency:
                    if word in freq:
                        freq[word] += 1
                    else:
                        freq[word] = 1
    if word_frequency:
        output_word_frequency(freq, f"{key}_word_frequency.txt")
    output_node_file(distinct, key)


def get_style_keys(path):
    """ Get Style node file. """
    data = pd.read_json(path, lines=True, chunksize=100000)
    styles = set()

    def add_key(style_dict):
        if isinstance(style_dict, dict):
            for key in style_dict:
                if key.endswith(':'):
                    key = key[:-1]
                styles.add(key.title())

    for chunk_id, chunk in enumerate(data):
        print(chunk_id * 100000 / REVIEW_COUNT * 100, '%')
        chunk["style"].apply(add_key)
    output_node_file(styles, 'style', col='key')


def get_reviewers(path):
    """
    Generate Reviewer node file.
    """
    reviewers = set()
    output_path = os.path.join(neo4j_import_dir, "reviewers.csv")
    with open(path, "r") as inf:
        for line in tqdm(inf, total=REVIEW_COUNT, desc="Line"):
            j = json.loads(line.strip())
            reviewers.add(
                (j["reviewerID"],
                 clean_html(j["reviewerName"]) if "reviewerName" in j else ""))
    with open(output_path, "w") as outf:
        outf.write("reviewerID:ID,name:String\n")
        for rid, name in sorted(reviewers):
            outf.write(f"{rid},{escape_comma(name)}\n")
        print(f"output to {output_path}")


def get_reviews(path):
    """ Generate Review node file. """
    chunksize = 1000000
    data = pd.read_json(path, lines=True, chunksize=chunksize)
    for chunk_id, chunk in enumerate(data):
        print((chunk_id + 1) * chunksize / REVIEW_COUNT * 100, '%')
        chunk["numImages"] = chunk["image"].str.len().fillna(0).astype(int)
        chunk.to_csv(os.path.join(neo4j_import_dir, f"review{chunk_id}.csv"),
                     header=[
                         "overall:Float", "unixReviewTime:Integer",
                         "verified:Boolean", "vote:Integer", "summary:String",
                         "reviewText:String", "numImages:Integer"
                     ],
                     columns=[
                         "overall", "unixReviewTime", "verified", "vote",
                         "summary", "reviewText", "numImages"
                     ],
                     index=True,
                     index_label="id:ID(reviewer_id)")


def get_product(data_path):
    """ Generate Product node file. """
    output_path = os.path.join(neo4j_import_dir, "product.csv")
    with open(data_path, "r") as inf:
        with open(output_path, "w") as outf:
            outf.write("asin:ID,description:List,price:String,rank:String\n")
            for line in tqdm(inf, total=META_COUNT, desc="Line"):
                j = json.loads(line.strip())
                asin = j["asin"]
                description = j["description"] if "description" in j else [""]
                description = "; ".join(description)
                description = escape_comma(description)
                price = j["price"] if "price" in j else ""
                price = escape_comma(price)
                rank = j["rank"] if "rank" in j else ""
                rank = rank[0] if isinstance(rank, list) else rank
                rank = escape_comma(rank)
                outf.write(f"{asin},{description},{price},{rank}\n")
    print(f"output to {output_path}")


def generate_node_files():
    """
    Generate the node files.
        * Brand
        * Category
        * Style
        * Reviewer
        * Review
        * Product
    """
    print("Generate node files")
    meta_path = os.path.join(root, "All_Amazon_Meta.json")
    review_path = os.path.join(root, "All_Amazon_Review.json")
    get_brands(meta_path,
               word_frequency=False,
               replace=[("Visit Amazon's", "Page"), ("(", ")"), ("\"", "\""),
                        ("{", "}")])
    get_categories(meta_path)
    get_style_keys(review_path)
    get_reviewers(review_path)
    get_reviews(review_path)
    get_product(meta_path)


if __name__ == "__main__":
    generate_node_files()
