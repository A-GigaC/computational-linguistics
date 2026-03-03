from typing import List, Dict, Any, Optional
from neo4j_driver import Neo4jDriver


class OntologyRepository:
    """Менеджер для работы с онтологией в Neo4j"""
    
    def __init__(self, driver: Neo4jDriver):
        self.driver = driver
    
    # ==================== ОНТОЛОГИЯ ====================
    
    def get_ontology(self) -> Dict[str, Any]:
        """Получить всю онтологию"""
        all_data = self.driver.get_all_nodes_and_arcs()
        
        ontology = {
            "classes": [],
            "objects": [],
            "datatype_properties": [],
            "object_properties": [],
            "class_hierarchy": [],
            "class_properties": [],
            "object_properties_range": []
        }
        
        for node in all_data["nodes"]:
            labels = node.get("label", "")
            if "Class" in labels:
                ontology["classes"].append(node)
            elif "Object" in labels:
                ontology["objects"].append(node)
            elif "DatatypeProperty" in labels:
                ontology["datatype_properties"].append(node)
            elif "ObjectProperty" in labels:
                ontology["object_properties"].append(node)
        
        for arc in all_data["arcs"]:
            arc_type = arc.get("uri", "")
            if arc_type == "subClassOf":
                ontology["class_hierarchy"].append(arc)
            elif arc_type == "domain":
                ontology["class_properties"].append(arc)
            elif arc_type == "range":
                ontology["object_properties_range"].append(arc)
        
        return ontology
    
    def get_ontology_parent_classes(self) -> List[Dict]:
        """Получить корневые классы онтологии"""
        query = """
        MATCH (c:Class)
        WHERE NOT EXISTS((c)<-[:subClassOf]-(:Class))
        RETURN c
        """
        result = self.driver.run_custom_query(query)
        return [self._convert_node_to_class(record["c"]) for record in result["data"]]
    
    # ==================== КЛАССЫ ====================
    
    def get_class(self, class_uri: str) -> Optional[Dict]:
        """Получить класс по URI"""
        node = self.driver.get_node_by_uri(class_uri)
        if node and "Class" in node.get("label", ""):
            return self._convert_node_to_class(node)
        return None
    
    def get_class_parents(self, class_uri: str) -> List[Dict]:
        """Получить родительские классы"""
        query = """
        MATCH (c:Class {uri: $uri})<-[:subClassOf]-(parent:Class)
        RETURN parent
        """
        result = self.driver.run_custom_query(query, {"uri": class_uri})
        return [self._convert_node_to_class(record["parent"]) for record in result["data"]]
    
    def get_class_children(self, class_uri: str) -> List[Dict]:
        """Получить дочерние классы"""
        query = """
        MATCH (c:Class {uri: $uri})-[:subClassOf]->(child:Class)
        RETURN child
        """
        result = self.driver.run_custom_query(query, {"uri": class_uri})
        return [self._convert_node_to_class(record["child"]) for record in result["data"]]
    
    def get_class_objects(self, class_uri: str, include_children: bool = True) -> List[Dict]:
        """Получить объекты класса"""
        if include_children:
            query = """
            MATCH (c:Class {uri: $uri})-[:subClassOf*0..]->(class:Class)<-[:rdf:type]-(obj:Object)
            RETURN obj
            """
        else:
            query = """
            MATCH (class:Class {uri: $uri})<-[:rdf:type]-(obj:Object)
            RETURN obj
            """
        result = self.driver.run_custom_query(query, {"uri": class_uri})
        return [self._convert_node_to_object(record["obj"]) for record in result["data"]]
    
    def create_class(self, title: str, description: str = "", parent_uri: Optional[str] = None) -> str:
        """Создать новый класс"""
        class_uri = self.driver.create_node({
            "labels": ["Class"],
            "title": title,
            "description": description
        })
        
        if parent_uri:
            self.add_class_parent(parent_uri, class_uri)
        
        return class_uri
    
    def update_class(self, class_uri: str, title: Optional[str] = None, description: Optional[str] = None) -> int:
        """Обновить класс"""
        params = {}
        if title is not None:
            params["title"] = title
        if description is not None:
            params["description"] = description
        
        if not params:
            return 0
        
        return self.driver.update_node(class_uri, params)
    
    def delete_class(self, class_uri: str, cascade: bool = True) -> int:
        """Удалить класс"""
        if cascade:
            query = """
            MATCH (c:Class {uri: $uri})-[:subClassOf*0..]->(class:Class)<-[:rdf:type]-(obj:Object)
            DETACH DELETE obj
            """
            self.driver.run_custom_query(query, {"uri": class_uri})
            
            query = """
            MATCH (c:Class {uri: $uri})-[:subClassOf*1..]->(child:Class)
            DETACH DELETE child
            """
            self.driver.run_custom_query(query, {"uri": class_uri})
        
        return self.driver.delete_node_by_uri(class_uri)
    
    def add_class_parent(self, parent_uri: str, target_uri: str) -> int:
        """Присоединить родителя к классу"""
        parent = self.get_class(parent_uri)
        target = self.get_class(target_uri)
        
        if not parent or not target:
            raise ValueError("Родительский или целевой класс не найден")
        
        arc_id = self.driver.create_arc(target_uri, parent_uri, "subClassOf")
        return arc_id
    
    # ==================== АТРИБУТЫ КЛАССА ====================
    
    def add_class_attribute(self, class_uri: str, attr_name: str, attr_description: str = "") -> str:
        """Добавить DatatypeProperty к классу"""
        prop_uri = self.driver.create_node({
            "labels": ["DatatypeProperty"],
            "title": attr_name,
            "description": attr_description
        })
        
        self.driver.create_arc(prop_uri, class_uri, "domain")
        
        return prop_uri
    
    def delete_class_attribute(self, property_uri: str) -> int:
        """Удалить DatatypeProperty у класса"""
        node = self.driver.get_node_by_uri(property_uri)
        if not node or "DatatypeProperty" not in node.get("label", ""):
            raise ValueError("Указанный URI не является DatatypeProperty")
        
        return self.driver.delete_node_by_uri(property_uri)
    
    def get_class_attributes(self, class_uri: str) -> List[Dict]:
        """Получить все DatatypeProperty класса"""
        query = """
        MATCH (c:Class {uri: $uri})-[:subClassOf*0..]->(class:Class)<-[:domain]-(prop:DatatypeProperty)
        RETURN prop
        """
        result = self.driver.run_custom_query(query, {"uri": class_uri})
        return [self._convert_node_to_property(record["prop"]) for record in result["data"]]
    
    # ==================== ОБЪЕКТНЫЕ АТРИБУТЫ ====================
    
    def add_class_object_attribute(self, class_uri: str, attr_name: str, range_class_uri: str, 
                                    attr_description: str = "") -> str:
        """Добавить ObjectProperty к классу"""
        range_class = self.get_class(range_class_uri)
        if not range_class:
            raise ValueError("Класс range не найден")
        
        prop_uri = self.driver.create_node({
            "labels": ["ObjectProperty"],
            "title": attr_name,
            "description": attr_description
        })
        
        self.driver.create_arc(prop_uri, class_uri, "domain")
        self.driver.create_arc(prop_uri, range_class_uri, "range")
        
        return prop_uri
    
    def delete_class_object_attribute(self, object_property_uri: str) -> int:
        """Удалить ObjectProperty"""
        node = self.driver.get_node_by_uri(object_property_uri)
        if not node or "ObjectProperty" not in node.get("label", ""):
            raise ValueError("Указанный URI не является ObjectProperty")
        
        return self.driver.delete_node_by_uri(object_property_uri)
    
    def get_class_object_attributes(self, class_uri: str) -> List[Dict]:
        """Получить все ObjectProperty класса"""
        query = """
        MATCH (c:Class {uri: $uri})-[:subClassOf*0..]->(class:Class)<-[:domain]-(prop:ObjectProperty)
        OPTIONAL MATCH (prop)-[:range]->(rangeClass:Class)
        RETURN prop, rangeClass
        """
        result = self.driver.run_custom_query(query, {"uri": class_uri})
        
        properties = []
        for record in result["data"]:
            prop_data = self._convert_node_to_property(record["prop"])
            if record.get("rangeClass"):
                prop_data["range_class"] = self._convert_node_to_class(record["rangeClass"])
            properties.append(prop_data)
        
        return properties
    
    # ==================== ОБЪЕКТЫ ====================
    
    def collect_signature(self, class_uri: str) -> Dict[str, Any]:
        """Сбор всех свойств класса"""
        signature = {
            "class_uri": class_uri,
            "datatype_properties": [],
            "object_properties": []
        }
        
        datatype_props = self.get_class_attributes(class_uri)
        signature["datatype_properties"] = datatype_props
        
        object_props = self.get_class_object_attributes(class_uri)
        signature["object_properties"] = object_props
        
        return signature
    
    def get_object(self, object_uri: str) -> Optional[Dict]:
        """Получить объект класса"""
        node = self.driver.get_node_by_uri(object_uri)
        if node and "Object" in node.get("label", ""):
            return self._convert_node_to_object(node)
        return None
    
    def create_object(self, class_uri: str, title: str, description: str = "", 
                      properties: Optional[Dict] = None) -> str:
        """Создать объект класса"""
        cls = self.get_class(class_uri)
        if not cls:
            raise ValueError("Класс не найден")
        
        object_uri = self.driver.create_node({
            "labels": ["Object", class_uri],
            "title": title,
            "description": description
        })
        
        self.driver.create_arc(object_uri, class_uri, "rdf:type")
        
        if properties:
            self._set_object_properties(object_uri, class_uri, properties)
        
        return object_uri
    
    def update_object(self, object_uri: str, title: Optional[str] = None, 
                      description: Optional[str] = None, properties: Optional[Dict] = None) -> int:
        """Обновить объект"""
        updated = 0
        
        params = {}
        if title is not None:
            params["title"] = title
        if description is not None:
            params["description"] = description
        
        if params:
            updated += self.driver.update_node(object_uri, params)
        
        if properties:
            obj = self.get_object(object_uri)
            if obj:
                class_uri = self._get_object_class_uri(object_uri)
                if class_uri:
                    self._set_object_properties(object_uri, class_uri, properties)
                    updated += 1
        
        return updated
    
    def delete_object(self, object_uri: str) -> int:
        """Удалить объект класса"""
        node = self.driver.get_node_by_uri(object_uri)
        if not node or "Object" not in node.get("label", ""):
            raise ValueError("Указанный URI не является Object")
        
        return self.driver.delete_node_by_uri(object_uri)
    
    # ==================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ====================
    
    def _convert_node_to_class(self, node: Dict) -> Dict:
        return {
            "uri": node.get("uri", ""),
            "title": node.get("title", ""),
            "description": node.get("description", ""),
            "type": "Class"
        }
    
    def _convert_node_to_object(self, node: Dict) -> Dict:
        return {
            "uri": node.get("uri", ""),
            "title": node.get("title", ""),
            "description": node.get("description", ""),
            "type": "Object"
        }
    
    def _convert_node_to_property(self, node: Dict) -> Dict:
        return {
            "uri": node.get("uri", ""),
            "title": node.get("title", ""),
            "description": node.get("description", ""),
            "type": "Property"
        }
    
    def _get_object_class_uri(self, object_uri: str) -> Optional[str]:
        query = """
        MATCH (obj:Object {uri: $uri})-[:rdf:type]->(cls:Class)
        RETURN cls.uri AS class_uri
        """
        result = self.driver.run_custom_query(query, {"uri": object_uri})
        if result["data"]:
            return result["data"][0]["class_uri"]
        return None
    
    def _set_object_properties(self, object_uri: str, class_uri: str, properties: Dict) -> None:
        signature = self.collect_signature(class_uri)
        
        for prop in signature["datatype_properties"]:
            prop_uri = prop["uri"]
            if prop_uri in properties:
                self.driver.update_node(object_uri, {prop_uri: properties[prop_uri]})
        
        for prop in signature["object_properties"]:
            prop_uri = prop["uri"]
            if prop_uri in properties:
                target_uri = properties[prop_uri]
                arc_type = prop["title"]
                self.driver.create_arc(object_uri, target_uri, arc_type)