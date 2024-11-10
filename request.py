import argparse
from neo4j import GraphDatabase

def run_query(query):
    db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4jlab4"))
    with db.session() as session:
        result = session.run(query)
        for record in result:
            print(record)
    db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Запросы к Neo4j")
    parser.add_argument("query_type", type=str, choices=["total_users", "total_groups", "top_users", "top_groups", "mutual_followers"],
                        help="Тип запроса")

    args = parser.parse_args()

    queries = {
        "total_users": "MATCH (u:User) RETURN count(u) AS total_users;",
        "total_groups": "MATCH (g:Group) RETURN count(g) AS total_groups;",
        "top_users": """
            MATCH (f:User)-[:FOLLOWS]->(u:User)
            RETURN u.id AS user_id, u.name AS name, count(f) AS followers_count
            ORDER BY followers_count DESC LIMIT 5;
        """,
        "top_groups": """
            MATCH (u:User)-[:MEMBER_OF]->(g:Group)
            RETURN g.id AS group_id, g.name AS name, count(u) AS member_count
            ORDER BY member_count DESC LIMIT 5;
        """,
        "mutual_followers": """
            MATCH (u1:User)-[:FRIENDS_WITH]-(u2:User)
            RETURN u1.id AS user1_id, u1.name AS user1_name, u2.id AS user2_id, u2.name AS user2_name;
        """
    }

    query = queries.get(args.query_type)
    run_query(query)