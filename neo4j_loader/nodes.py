#! /usr/bin/env python3
""" Generate node files for Amazon product review data. """

import json
import os
from datetime import datetime
import pandas as pd
from tqdm import tqdm

from utils import *


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
    distinct = {}
    freq = {}
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
    freq = {}
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
                styles.add(clean_style_key(key))

    for chunk_id, chunk in enumerate(data):
        print(chunk_id * 100000 / REVIEW_COUNT * 100, '%')
        chunk["style"].apply(add_key)
    output_node_file(styles, 'style', col='key')


def get_reviewers(path):
    """
    Generate Reviewer node file.
    """
    reviewers = {}
    output_path = os.path.join(neo4j_import_dir, "reviewers.csv")
    with open(path, "r") as inf:
        for line in tqdm(inf, total=REVIEW_COUNT, desc="Line"):
            j = json.loads(line.strip())
            if not j["reviewerID"] in reviewers:
                name = escape_comma_newline(clean_html(
                    j["reviewerName"])) if "reviewerName" in j else ""
                reviewers[j["reviewerID"]] = name
    with open(output_path, "w") as outf:
        outf.write("reviewerID:ID,name:string\n")
        for rid, name in sorted(reviewers.items()):
            outf.write(f"{rid},{escape_comma_quote(name)}\n")
        print(f"output to {output_path}")


def get_reviews(path):
    """ Generate Review node file per year. """
    # create review folder
    if not os.path.exists(os.path.join(neo4j_import_dir, 'review')):
        os.mkdir(os.path.join(neo4j_import_dir, 'review'))
    # write header in a separate file
    with open(os.path.join(neo4j_import_dir, 'review', 'review_header.csv'),
              'w') as outf:
        outf.write(
            "id:ID(review_id),overall:float,unixReviewTime:int,"
            "verified:boolean,vote:int,summary:string,reviewText:string,"
            "numImages:int\n")
    with [
            open(os.path.join(neo4j_import_dir, 'review', f"review{year}.csv"),
                 'w') for year in range(1996, 2019)
    ] as outfiles:
        line_counts = [0 for outf in outfiles]
        with open(path, 'r') as inf:
            for line in tqdm(inf, total=REVIEW_COUNT, desc="Line"):
                j = json.loads(line.strip())
                # get fields
                overall = j['overall'] if 'overall' in j else ''
                time = j['unixReviewTime'] if 'unixReviewTime' in j else ''
                verified = j['verified'] if 'verified' in j else ''
                vote = j['vote'].replace(',', '') if 'vote' in j else ''
                summary = escape_comma_newline(
                    j['summary']) if 'summary' in j else ''
                review_text = escape_comma_newline(
                    j['reviewText']) if 'reviewText' in j else ''
                num_images = len(j['image']) if 'image' in j else 0
                # output
                try:
                    year = datetime.fromtimestamp(int(time)).year
                except ValueError:
                    year = 1996
                idx = line_counts[year - 1996]
                outfiles[year - 1996].write(
                    f"R{year}{idx},{overall},{time},{verified},{vote},{summary},{review_text},{num_images}\n"
                )
                line_counts[year - 1996] += 1
    print(f"output to {os.path.join(neo4j_import_dir, 'review')}")


def get_product(data_path):
    """ Generate Product node file. """
    output_path = os.path.join(neo4j_import_dir, "product.csv")
    asins = set()
    extended_similar_asins = set()
    with open(data_path, "r") as inf:
        with open(output_path, "w") as outf:
            outf.write(
                "asin:ID,description:string[],price:string,rank:string\n")
            for line in tqdm(inf, total=META_COUNT, desc="Line"):
                j = json.loads(line.strip())
                asin = j["asin"]
                if asin in asins:  # avoid repeated product entry
                    continue
                asins.add(asin)
                description = j["description"] if "description" in j else [""]
                description = [
                    desc.strip() for desc in description
                    if len(desc.strip()) > 0
                ]
                description = "; ".join(description)
                description = escape_comma_newline(description)
                price = j["price"] if "price" in j else ""
                price = escape_comma_newline(price)
                rank = j["rank"] if "rank" in j else ""
                rank = rank[0] if isinstance(rank, list) else rank
                rank = escape_comma_newline(rank)
                outf.write(f"{asin},{description},{price},{rank}\n")

                # handle product that only exist in similar item info
                for key in ['also_buy', 'also_view', 'similar_item']:
                    if key in j:
                        ids = j[key]
                        if key == "similar_item":
                            ids = [
                                subj["asin"] for subj in ids if "asin" in subj
                            ]
                        for similar_asin in ids:
                            if len(
                                    similar_asin
                            ) > 0 and similar_asin != 'new-releases' and similar_asin not in asins:
                                extended_similar_asins.add(similar_asin)
            for asin in sorted(extended_similar_asins):
                if asin not in asins:
                    outf.write(f"{asin},,,\n")
    print(f"output to {output_path}")


def get_missing_products(rates_file,
                         product_files,
                         output_name='missing_product.csv'):
    """
    @param rates_file The Review_rates_Product.csv file, with header "review_id,product_id"
    @param product_files The product.csv file, with header "asin,description,price,rank"
    @param output_name The output name
    """
    rated_products = set()
    with open(rates_file, 'r') as inf:
        for line in tqdm(inf, desc='Line'):
            _, asin = line.strip().rsplit(',', 1)
            rated_products.add(asin)
    rated_products.remove(':END_ID')
    print(f"rated_products {len(rated_products)}")

    asins = set()
    for product_file in product_files:
        print(f"processing {product_file}")
        with open(product_file, 'r') as inf:
            for line in inf:
                asin, _ = line.split(',', 1)
                asins.add(asin)
    with open(os.path.join(neo4j_import_dir, output_name), 'w') as outf:
        for asin in rated_products:
            if asin not in asins:
                outf.write(f'{asin},,,\n')


def generate_node_files(meta_path, review_path):
    """
    Generate the node files.
        * Brand
        * Category
        * Style
        * Reviewer
        * Review
        * Product
    """
    print(f'meta_path={meta_path}')
    print(f'review_path={review_path}')

    print("Generate Brand node files")
    get_brands(meta_path, word_frequency=False, replace=BRAND_REPLACE_PATTERNS)
    print("Generate Category node files")
    get_categories(meta_path)
    print("Generate Style node files")
    get_style_keys(review_path)
    print("Generate Reviewer node files")
    get_reviewers(review_path)
    print("Generate Product node files")
    get_product(meta_path)
    print("Generate Review node files")
    get_reviews(review_path)


if __name__ == "__main__":
    generate_node_files(os.path.join(root, "All_Amazon_Meta.json"),
                        os.path.join(root, "All_Amazon_Review.json"))
