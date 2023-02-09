#! /usr/bin/env python3
""" Import Amazon product review graph to NEO4J using neo4j-admin import. """

import os
from glob import glob

from utils import neo4j_import_dir

neo4j_import_dir = os.path.join(os.getenv("NEO4J_HOME"),
                                "import/subgraph/Appliances/v1")

###### NODE FILES ######
# excluding reviews of year 2018
# if multiple CSVs in a file group contain header, use --auto-skip-subsequent-headers
PRODUCT_FILES = ','.join(glob("*product_with_rate_price.csv", root_dir=neo4j_import_dir))
NODE_FILES = [
    f'--nodes=Product={PRODUCT_FILES}',
    '--nodes=Reviewer=reviewers.csv',
]

###### RELATIONSHIP FILES ######
RELATIONSHIP_FILES = [
    "--relationships=isSimilarTo=Product_isSimilarTo_Product.csv",
    "--relationships=alsoBuy=Product_alsoBuy_Product.csv",
    "--relationships=alsoView=Product_alsoView_Product.csv",
    "--relationships=sameRates=User_usu_User.csv",
    "--relationships=rates=Reviewer_rates_Product.csv"
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
    legacy = '--database=demo1 \\\n'
    cmd = f"neo4j-admin import {legacy}" + ' \\\n'.join(NODE_FILES +
                                                        RELATIONSHIP_FILES)
    print(cmd)
    os.system(cmd)


main()
