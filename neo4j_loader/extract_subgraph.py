import os
from neo4j import GraphDatabase
from utils import *

class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def eval(self, fn_name):
        return eval(f"self.{fn_name}")

    def execute(self, fn_name, category):
        with self.driver.session() as session:
            session.execute_read(self.eval(fn_name), category)

    @staticmethod
    def _extract_product(tx, category):
        """
        The function extracts all nodes of type "Product" that 
        belong to the specified category, and exports the data to a CSV file.
        Parameters:
            tx: neo4j transaction
            category: category name
        """
        result = tx.run("MATCH (p: Product)-[:belongsTo]->(c:Category {name: $category})" 
                        "RETURN p", category=category)

        output_folder = os.path.join(neo4j_import_dir, "subgraph", category[1:])
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        output_path = os.path.join(output_folder, "product.csv")
        distinct = set()
        with open(output_path, "w") as outf:
            outf.write(
                "asin:ID,description:string[],price:string,rank:string\n")
            for record in result:
                j = dict(record["p"])
                asin = j["asin"]
                description = j["description"] if "description" in j else [""]
                description = [
                    desc.strip() for desc in description
                    if len(desc.strip()) > 0
                ]
                description = "; ".join(description)
                description = escape_comma_newline(description)
                price = escape_comma_newline(j["price"]) if "price" in j else ""
                rank = escape_comma_newline(j["rank"]) if "rank" in j else ""
                distinct.add((asin, description, price, rank))

            for asin, description, price, rank in distinct:
                outf.write(f"{asin},{description},{price},{rank}\n")

    @staticmethod
    def _extract_reviewer(tx, category):
        """
        The function extracts all nodes of type "Reviewer" that 
        belong to the specified category, and exports the data to a CSV file.
        Parameters:
            tx: neo4j transaction
            category: category name
        """
        result = tx.run("MATCH (r:Reviewer)<-[:isWrittenBy]-(:Review)-[:rates]->(p:Product)-[:belongsTo]->(c:Category {name: $category})" 
                        "RETURN r", category=category)

        output_folder = os.path.join(neo4j_import_dir, "subgraph", category[1:])
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        output_path = os.path.join(output_folder, "reviewers.csv")
        distinct = set()
        with open(output_path, "w") as outf:
            outf.write("reviewerID:ID,name:string\n")
            for record in result:
                j = dict(record["r"])
                id = j["reviewerID"]
                name = escape_comma_newline(j['name']) if 'name' in j else ''
                distinct.add((id, name))

            for id, name in distinct:
                outf.write(f"{id},{name}\n")

    @staticmethod
    def _extract_product_to_product(tx, category):
        """
        The function extracts all edges of types ["isSimilarTo", "alsoBuy", "alsoView"] that 
        belong to the specified category, and exports the data to a CSV file.
        Parameters:
            tx: neo4j transaction
            category: category name
        """
        relations = ["isSimilarTo", "alsoBuy", "alsoView"]
        cypher_query = {
            "isSimilarTo": "MATCH (:Category {name: $category})<-[:belongsTo]-(p1:Product)-[r:isSimilarTo]->(p2:Product)-[:belongsTo]->(:Category {name: $category}) RETURN DISTINCT p1, p2",
            "alsoBuy": "MATCH (:Category {name: $category})<-[:belongsTo]-(p1:Product)-[r:alsoBuy]->(p2:Product)-[:belongsTo]->(:Category {name: $category}) RETURN DISTINCT p1, p2",
            "alsoView": "MATCH (:Category {name: $category})<-[:belongsTo]-(p1:Product)-[r:alsoView]->(p2:Product)-[:belongsTo]->(:Category {name: $category}) RETURN DISTINCT p1, p2"
            }
        for relation in relations:
            result = tx.run(cypher_query[relation], category=category)

            output_folder = os.path.join(neo4j_import_dir, "subgraph", category[1:])
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
            output_path = os.path.join(output_folder, 
                                f"Product_{relation}_Product.csv")
            with open(output_path, "w") as outf:
                outf.write(":START_ID,:END_ID\n")
                for record in result:
                    j_p1 = dict(record["p1"])
                    j_p2 = dict(record["p2"])
                    asin1 = j_p1["asin"]
                    asin2 = j_p2["asin"]
                    outf.write(f"{asin1},{asin2}\n")

    @staticmethod
    def _extract_usu(tx, category):
        """
        The function extracts all edges of types "sameRates" that 
        belong to the specified category, and exports the data to a CSV file.
        The "sameRates" edge type links Reviewers who have given at least one common star rating.
        Parameters:
            tx: neo4j transaction
            category: category name
        """
        result = tx.run("MATCH (u1: Reviewer)<-[:isWrittenBy]-(r1:Review)-[:rates]->(:Product)<-[:rates]-(r2:Review)-[:isWrittenBy]->(u2: Reviewer) WHERE r1.overall=r2.overall RETURN DISTINCT u1, u2")

        output_folder = os.path.join(neo4j_import_dir, "subgraph", category[1:], "v1")
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        output_path = os.path.join(output_folder, 
                               "User_usu_User.csv")
        with open(output_path, "w") as outf:
            outf.write(":START_ID,:END_ID\n")
            for record in result:
                j_u1 = dict(record["u1"])
                j_u2 = dict(record["u2"])
                user1_id = j_u1["reviewerID"]
                user2_id = j_u2["reviewerID"]
                outf.write(f"{user1_id},{user2_id}\n")

    @staticmethod
    def _extract_itemprod(tx, category):
        """
        The function extracts all edges of types "rates" that 
        belong to the specified category, and exports the data to a CSV file.
        The "rates" edge type connects Reviewer nodes with the Product nodes they have rated.
        Parameters:
            tx: neo4j transaction
            category: category name
        """
        result = tx.run("MATCH (u: Reviewer)<-[:isWrittenBy]-(: Review)-[:rates]->(p: Product) RETURN u, p")

        output_folder = os.path.join(neo4j_import_dir, "subgraph", category[1:], "v1")
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        output_path = os.path.join(output_folder,
                                 "User_itemprod_Product.csv")
        with open(output_path, "w") as outf:   
            outf.write(":START_ID,:END_ID\n")
            for record in result:
                j_u = dict(record["u"])
                j_p = dict(record["p"])
                user_id = j_u["reviewerID"]
                product_id = j_p["asin"]
                outf.write(f"{user_id},{product_id}\n")

if __name__ == "__main__":
    client = Neo4jHandler("bolt://localhost:7687", "neo4j", "neo4j")
    client.execute("_extract_product", " Appliances")
    client.execute("_extract_reviewer", " Appliances")
    client.execute("_extract_product_to_product", " Appliances")
    client.execute("_extract_usu", " Appliances")
    client.execute("_extract_itemprod", " Appliances")
    client.close()
