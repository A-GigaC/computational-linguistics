import uuid
import re
from typing import List, Dict, Any, Optional, Tuple
from neo4j import GraphDatabase, Driver, exceptions
from neo4j.graph import Node, Relationship

class Neo4jDriver:

    def __init__(self, uri: str, user: str, password: str):
        try:
            self.driver: Driver = GraphDatabase.driver(uri, auth=(user, password))
            self.driver.verify_connectivity()
        except Exception as e:
            raise ConnectionError(f"Не удалось подключиться к Neo4j: {str(e)}")
    
    def close(self):
        if self.driver:
            self.driver.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    @staticmethod
    def _validate_label(label: str) -> bool:
        return bool(re.match(r'^[A-Za-z][A-Za-z0-9_]*$', label))
    
    @staticmethod
    def generate_random_string() -> str:
        return uuid.uuid4().hex
    
    def _run_query(self, query: str, parameters: Optional[Dict] = None) -> Tuple[List, Any]:
        if parameters is None:
            parameters = {}
        
        try:
            with self.driver.session() as session:
                result = session.run(query, parameters)
                records = [record for record in result]
                summary = result.consume()
                return records, summary
        except exceptions.CypherSyntaxError as e:
            raise ValueError(f"Ошибка синтаксиса Cypher: {str(e)}")
        except exceptions.ConstraintError as e:
            raise ValueError(f"Ошибка ограничения базы данных: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Ошибка выполнения запроса: {str(e)}")
    
    def collect_node(self, node: Node) -> Dict[str, str]:
        if not isinstance(node, Node):
            raise TypeError("Аргумент должен быть объектом neo4j.graph.Node")
        
        labels = ','.join(sorted(node.labels)) if node.labels else ""
        return {
            "uri": node.get("uri", ""),
            "description": node.get("description", ""),
            "label": labels  # Все метки через запятую
        }
    
    def collect_arc(self, record: Any) -> Dict[str, Any]:
        rel = record.get("r")
        if not isinstance(rel, Relationship):
            raise TypeError("Поле 'r' должно быть объектом Relationship")
        
        return {
            "id": rel.id,
            "uri": rel.type,  # Тип связи (например, "RELATED")
            "node_uri_from": record.get("from_uri", ""),
            "node_uri_to": record.get("to_uri", "")
        }
    
    def get_all_nodes_and_arcs(self) -> Dict[str, List]:
        # Получаем все узлы с uri
        nodes_query = "MATCH (n) WHERE n.uri IS NOT NULL RETURN n"
        node_records, _ = self._run_query(nodes_query)
        nodes = [self.collect_node(record["n"]) for record in node_records]
        
        # Получаем все связи с uri узлов-источника и назначения
        arcs_query = """
        MATCH (a)-[r]-(b)
        WHERE a.uri IS NOT NULL AND b.uri IS NOT NULL
        RETURN r, startnode(r).uri AS from_uri, endnode(r).uri AS to_uri
        """
        arc_records, _ = self._run_query(arcs_query)
        arcs = [self.collect_arc(record) for record in arc_records]
        
        return {"nodes": nodes, "arcs": arcs}
    
    def get_nodes_by_labels(self, labels: List[str]) -> List[Dict]:
        if not labels:
            raise ValueError("Список меток не может быть пустым")
        
        # Валидация меток
        for label in labels:
            if not self._validate_label(label):
                raise ValueError(f"Некорректная метка: {label}")
        
        # Формируем условие для запроса (безопасно благодаря валидации)
        label_conditions = " AND ".join([f"n:`{label}`" for label in labels])
        query = f"""
        MATCH (n)
        WHERE {label_conditions} AND n.uri IS NOT NULL
        RETURN n
        """
        
        records, _ = self._run_query(query)
        return [self.collect_node(record["n"]) for record in records]
    
    def get_node_by_uri(self, uri: str) -> Optional[Dict]:
        query = "MATCH (n {uri: $uri}) RETURN n"
        records, _ = self._run_query(query, {"uri": uri})
        return self.collect_node(records[0]["n"]) if records else None
    
    def create_node(self, params: Dict) -> str:
        uri = self.generate_random_string()
        labels = params.get("labels", [])
        
        # Валидация меток
        for label in labels:
            if not self._validate_label(label):
                raise ValueError(f"Некорректная метка при создании узла: {label}")
        
        # Cтроку меток для запроса
        label_str = ":" + ":".join(labels) if labels else ""
        
        # Подготавливаем свойства (без служебных ключей)
        properties = {
            k: v for k, v in params.items() 
            if k not in ["labels", "uri"] and not k.startswith("_")
        }
        properties["uri"] = uri
        
        query = f"""
        CREATE (n{label_str} $properties)
        RETURN n.uri AS uri
        """
        
        records, _ = self._run_query(query, {"properties": properties})
        return records[0]["uri"] if records else uri  # uri гарантированно существует
    
    def create_arc(self, node1_uri: str, node2_uri: str, arc_type: str = "RELATED") -> int:
        if not self._validate_label(arc_type):
            raise ValueError(f"Некорректный тип связи: {arc_type}")
        
        query = f"""
        MATCH (a {{uri: $uri1}}), (b {{uri: $uri2}})
        CREATE (a)-[r:`{arc_type}`]->(b)
        RETURN id(r) AS rel_id
        """
        
        records, _ = self._run_query(query, {"uri1": node1_uri, "uri2": node2_uri})
        if not records:
            raise RuntimeError("Не удалось создать связь. Проверьте URI узлов.")
        return records[0]["rel_id"]
    
    def delete_node_by_uri(self, uri: str) -> int:
        query = "MATCH (n {uri: $uri}) DETACH DELETE n"
        _, summary = self._run_query(query, {"uri": uri})
        return summary.counters.nodes_deleted
    
    def delete_arc_by_id(self, arc_id: int) -> int:
        query = "MATCH ()-[r]-() WHERE id(r) = $arc_id DELETE r"
        _, summary = self._run_query(query, {"arc_id": arc_id})
        return summary.counters.relationships_deleted
    
    def update_node(self, node_uri: str, params: Dict) -> int:
        # Исключаем служебные поля
        clean_params = {
            k: v for k, v in params.items() if k not in ["uri", "labels"] and not k.startswith("_")
        }
        
        if not clean_params:
            return 0
        
        query = """
        MATCH (n {uri: $node_uri})
        SET n += $properties
        RETURN count(n) AS updated
        """
        
        _, summary = self._run_query(query, {
            "node_uri": node_uri,
            "properties": clean_params
        })
        return summary.counters.properties_set // len(clean_params) if clean_params else 0
    
    def run_custom_query(self, query: str, parameters: Optional[Dict] = None) -> Dict:
        if not query or not query.strip():
            raise ValueError("Запрос не может быть пустым")
        
        records, summary = self._run_query(query, parameters)
        
        # Преобразуем записи в читаемый формат
        data = []
        for record in records:
            clean_record = {}
            for key in record.keys():
                value = record[key]
                # Преобразуем Node/Relationship в словари для сериализации
                if isinstance(value, Node):
                    clean_record[key] = dict(value)
                elif isinstance(value, Relationship):
                    clean_record[key] = {
                        "id": value.id,
                        "type": value.type,
                        "start_node": dict(value.start_node) if hasattr(value.start_node, "items") else str(value.start_node),
                        "end_node": dict(value.end_node) if hasattr(value.end_node, "items") else str(value.end_node)
                    }
                else:
                    clean_record[key] = value
            data.append(clean_record)
        
        return {
            "data": data,
            "summary": {
                "nodes_created": summary.counters.nodes_created,
                "nodes_deleted": summary.counters.nodes_deleted,
                "relationships_created": summary.counters.relationships_created,
                "relationships_deleted": summary.counters.relationships_deleted,
                "properties_set": summary.counters.properties_set
            }
        }