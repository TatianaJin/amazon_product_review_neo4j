#! /usr/bin/env python3
""" Preprocess Amazon product review data into node and relationship files"""

import nodes
import relationships

nodes.generate_node_files()
relationships.generate_relationship_files()
