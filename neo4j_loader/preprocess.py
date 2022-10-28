#! /usr/bin/env python3
""" Preprocess Amazon product review data into node and relationship files"""

import os

import nodes
import relationships
from utils import neo4j_import_dir

nodes.generate_node_files()
relationships.generate_relationship_files()
nodes.get_missing_products(
    os.path.join(neo4j_import_dir, 'Review_rates_Product.csv'), [
        os.path.join(neo4j_import_dir, 'product.csv'),
        os.path.join(neo4j_import_dir, 'extended_product.csv')
    ])
