#! /usr/bin/env python3
""" Generate relationship files for Amazon product review data. """

import os
import json
from datetime import datetime
import pandas as pd
from tqdm import tqdm

from utils import *


def has_brand(meta_path, brand_file_name="brand.csv"):
    """
    Generate Product_hasBrand_Brand relationship file.
    """
    brand_path = os.path.join(neo4j_import_dir, brand_file_name)
    output_path = os.path.join(neo4j_import_dir, "Product_hasBrand_Brand.csv")

    # load brand id to name dict
    print(f"read brands {brand_path}")
    name_col = 'name:string'
    brands = pd.read_csv(brand_path,
                         converters={
                             'name:string': str,
                             'id:ID(brand_id)': str
                         })
    brands[name_col] = brands[name_col].apply(lambda x: simplify_value(x)[0])
    brands = brands.set_index(name_col)
    # compute edges
    with open(meta_path, "r") as inf:
        with open(output_path, 'w') as outf:
            outf.write(":START_ID,:END_ID(brand_id)\n")
            for linenum, line in tqdm(enumerate(inf),
                                      total=META_COUNT,
                                      desc="Line"):
                j = json.loads(line.strip())
                if 'brand' in j:
                    value = clean_brand_values(j['brand'].strip(),
                                               BRAND_REPLACE_PATTERNS)
                    if value is None:
                        continue
                    signature, value = simplify_value(value)
                    assert len(j['asin']) > 0
                    assert signature in brands.index, f"{linenum},{j['brand']},{value},{signature}."
                    brand_id = brands.loc[signature]["id:ID(brand_id)"]
                    outf.write(f"{j['asin']},{brand_id}\n")
        print(f"output to {output_path}")


def get_review_id(j, line_counts):
    """ Get review id by year and line count. """
    time = j['unixReviewTime'] if 'unixReviewTime' in j else ''
    try:
        year = datetime.fromtimestamp(int(time)).year
    except ValueError:
        year = 1996
    return f"R{year}{line_counts[year-1996]}", year


def is_written_by(data_path):
    """ Generate relationship file Review_isWrittenBy_Reviewer.csv. """
    output_path = os.path.join(neo4j_import_dir,
                               "Review_isWrittenBy_Reviewer.csv")
    inc_path = os.path.join(neo4j_import_dir,
                            "Review_isWrittenBy_Reviewer_2018.csv")
    line_counts = [0 for year in range(1996, 2019)]  # line count per year
    with open(output_path, "w") as outf, open(inc_path, "w") as incf:
        outf.write(":START_ID(review_id),:END_ID\n")
        incf.write(":START_ID(review_id),:END_ID\n")
        with open(data_path, "r") as inf:
            for line in tqdm(inf, total=REVIEW_COUNT, desc="Line"):
                j = json.loads(line.strip())
                review_id, year = get_review_id(j, line_counts)
                if "reviewerID" in j and len(j["reviewerID"]) > 0:
                    reviewer_id = j["reviewerID"]
                    if year == 2018:
                        incf.write(f"{review_id},{reviewer_id}\n")
                    else:
                        outf.write(f"{review_id},{reviewer_id}\n")
                line_counts[year - 1996] += 1
    print(f"static data output to {output_path}")
    print(f"increment data output to {inc_path}")


def refers_to(data_path, style_file_name='style.csv'):
    """ Generate relationship file Review_refersTo_Style.csv """
    # load style id to key dict
    styles = pd.read_csv(os.path.join(neo4j_import_dir, style_file_name),
                         converters={
                             'key:string': str,
                             'id:ID(style_id)': str
                         },
                         index_col='key:string')

    # prepare output files
    output_path = os.path.join(neo4j_import_dir, "Review_refersTo_Style.csv")
    inc_path = os.path.join(neo4j_import_dir, "Review_refersTo_Style_2018.csv")
    line_counts = [0 for year in range(1996, 2019)]  # line count per year
    with open(output_path, 'w') as outf, open(inc_path, 'w') as incf:
        outf.write(":START_ID(review_id),value:string,:END_ID(style_id)\n")
        incf.write(":START_ID(review_id),value:string,:END_ID(style_id)\n")

        # compute edges
        with open(data_path, "r") as inf:
            for line in tqdm(inf, total=REVIEW_COUNT, desc="Line"):
                j = json.loads(line.strip())
                review_id, year = get_review_id(j, line_counts)
                if "style" in j:
                    if isinstance(j["style"], dict):
                        for key in j["style"]:
                            # edge weight and dst node
                            value = escape_comma_newline(
                                j["style"][key].strip())
                            style_id = styles.loc[clean_style_key(
                                key)]['id:ID(style_id)']
                            if year == 2018:
                                incf.write(f"{review_id},{value},{style_id}\n")
                            else:
                                outf.write(f"{review_id},{value},{style_id}\n")
                line_counts[year - 1996] += 1
    print(f"static data output to {output_path}")
    print(f"increment data output to {inc_path}")


def rates(data_path):
    """ Generate relationship file Review_rates_Product.csv """
    # prepare output files
    output_path = os.path.join(neo4j_import_dir, "Review_rates_Product.csv")
    inc_path = os.path.join(neo4j_import_dir, "Review_rates_Product_2018.csv")
    line_counts = [0 for year in range(1996, 2019)]  # line count per year
    with open(output_path, 'w') as outf, open(inc_path, 'w') as incf:
        outf.write(":START_ID(review_id),:END_ID\n")
        incf.write(":START_ID(review_id),:END_ID\n")

        # compute edges
        with open(data_path, "r") as inf:
            for line in tqdm(inf, total=REVIEW_COUNT, desc="Line"):
                j = json.loads(line.strip())
                review_id, year = get_review_id(j, line_counts)
                if "asin" in j:
                    if year == 2018:
                        incf.write(f"{review_id},{j['asin']}\n")
                    else:
                        outf.write(f"{review_id},{j['asin']}\n")
                line_counts[year - 1996] += 1
    print(f"static data output to {output_path}")
    print(f"increment data output to {inc_path}")


def belongs_to(data_path, category_file_name='category.csv'):
    """ Generate relationship file Product_belongsTo_Category.csv """
    # load category id to name dict
    name_col, id_col = 'name:string', 'id:ID(category_id)'
    categories = pd.read_csv(os.path.join(neo4j_import_dir,
                                          category_file_name),
                             converters={
                                 name_col: str,
                                 id_col: str
                             })
    categories[name_col] = categories[name_col].str.strip()
    categories = categories.set_index(name_col)

    # compute edges
    output_path = os.path.join(neo4j_import_dir,
                               "Product_belongsTo_Category.csv")
    with open(data_path, "r") as inf:
        with open(output_path, 'w') as outf:
            outf.write(":START_ID,:END_ID(category_id)\n")
            for line in tqdm(inf, total=META_COUNT, desc="Line"):
                j = json.loads(line.strip())
                if "category" in j:
                    # we only use the first category and force the first category
                    # to be in the category list
                    value = j["category"][0].replace(",", " &")
                    assert value in categories.index, "category not in category list"
                    category_id = categories.loc[value][id_col]
                    outf.write(f"{j['asin']},{category_id}\n")
    print(f"output to {output_path}")


def product_to_product(data_path, key="also_buy"):
    """ Generate relationship files for product-product relations.
        @param key Can be a key string or a list of keys.
    """
    edge_map = {
        "similar_item": "isSimilarTo",
        "also_buy": "alsoBuy",
        "also_view": "alsoView"
    }
    if not isinstance(key, list):
        key = [key]
    out_files = [f"Product_{edge_map[k]}_Product.csv" for k in key]
    with [open(os.path.join(neo4j_import_dir, x), 'r')
          for x in out_files] as out_files:
        for outf in out_files:
            outf.write(":START_ID,:END_ID\n")

        with open(data_path, "r") as inf:
            for line in tqdm(inf, total=META_COUNT, desc="Line"):
                j = json.loads(line.strip())
                for k, out_file in zip(key, out_files):
                    if k in j:
                        data_list = j[k]
                        if k == "similar_item":
                            data_list = [
                                j["asin"] for j in data_list if "asin" in j
                            ]
                        for dst_asin in data_list:
                            if len(dst_asin
                                   ) == 0 or dst_asin == "new-releases":
                                continue
                            out_file.write(f"{j['asin']},{dst_asin}\n")


def generate_relationship_files():
    """
    Generate the relationship files.
        * Review_isWrittenBy_Reviewer
    """
    print("Generate relationship files")
    meta_path = os.path.join(root, "All_Amazon_Meta.json")
    review_path = os.path.join(root, "All_Amazon_Review.json")
    has_brand(meta_path)
    is_written_by(review_path)
    refers_to(review_path)
    rates(review_path)
    belongs_to(meta_path)
    product_to_product(meta_path, ['also_buy', 'also_view', 'similar_item'])


if __name__ == "__main__":
    generate_relationship_files()
