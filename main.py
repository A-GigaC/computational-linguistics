from neo4j_driver import Neo4jDriver


with Neo4jDriver("bolt://localhost:7687", "neo4j", "passpass") as driver:
    # Создание узла с метками
    uri1 = driver.create_node({
        "labels": ["Person"],
        "name": "Alice",
        "description": "Developer"
    })
    
    uri2 = driver.create_node({
        "labels": ["Company"],
        "name": "Neo4j Inc",
        "description": "Graph DB Company"
    })
    
    arc_id = driver.create_arc(uri1, uri2, "WORKS_AT")
    
    node = driver.get_node_by_uri(uri1)
    if node is not None:
        print(node) 
        
    driver.update_node(uri1, {"age": 30})
    
    driver.delete_arc_by_id(arc_id)
    driver.delete_node_by_uri(uri2)