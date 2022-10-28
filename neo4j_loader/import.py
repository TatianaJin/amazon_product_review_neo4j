#! /usr/bin/env python3
""" Import Amazon product review graph to NEO4J using neo4j-admin import. """

import os

from utils import neo4j_import_dir

###### NODE FILES ######
# excluding reviews of year 2018
# if multiple CSVs in a file group contain header, use --auto-skip-subsequent-headers
NODE_FILES = [
    '--nodes=Brand=brand.csv', '--nodes=Category=category.csv',
    '--nodes=Style=style.csv', '--nodes=Product=product.csv',
    '--nodes=Reviewer=reviewers.csv',
    '--nodes=Review=review/review_header.csv,' +
    ','.join([f'review/review{year}.csv' for year in range(1996, 2018)])
]

###### RELATIONSHIP FILES ######
RELATIONSHIP_FILES = [
    "--relationships=isWrittenBy=Review_isWrittenBy_Reviewer.csv",
    "--relationships=refersTo=Review_refersTo_Style.csv",
    "--relationships=rates=Review_rates_Product.csv",
    "--relationships=belongsTo=Product_belongsTo_Category.csv",
    "--relationships=hasBrand=Product_hasBrand_Brand.csv",
    "--relationships=isSimilarTo=Product_isSimilarTo_Product.csv",
    "--relationships=alsoBuy=Product_alsoBuy_Product.csv",
    "--relationships=alsoView=Product_alsoView_Product.csv"
]


def validate_paths(files):
    """ Check that paths exist. """
    for paths in files:
        for path in paths.rsplit('=', 1)[-1].split(','):
            assert os.path.exists(path), f"File not exists: {path}."


def main():
    """ neo4j-admin import """
    os.chdir(neo4j_import_dir)
    validate_paths(NODE_FILES)
    validate_paths(RELATIONSHIP_FILES)
    # legacy = '--legacy-style-quoting=true \\\n'
    legacy = ''
    cmd = f"neo4j-admin import {legacy}" + ' \\\n'.join(NODE_FILES +
                                                        RELATIONSHIP_FILES)
    print(cmd)
    os.system(cmd)


main()
