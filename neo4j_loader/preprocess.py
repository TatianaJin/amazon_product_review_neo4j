#! /usr/bin/env python3
""" Preprocess Amazon product review data into node and relationship files"""

import os

import nodes
import relationships
from utils import root, neo4j_import_dir


def main(meta_path=os.path.join(root, "All_Amazon_Meta.json"),
         review_path=os.path.join(root, "All_Amazon_Review.json")):
    """ main function """
    nodes.generate_node_files(meta_path, review_path)
    relationships.generate_relationship_files(meta_path, review_path)
    nodes.get_missing_products(
        os.path.join(neo4j_import_dir, 'Review_rates_Product.csv'), [
            os.path.join(neo4j_import_dir, 'product.csv'),
            os.path.join(neo4j_import_dir, 'extended_product.csv')
        ])


if __name__ == "__main__":
    main()
