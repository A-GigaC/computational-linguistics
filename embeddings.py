import numpy as np
from typing import List
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


def get_chunks(text: str, chunk_size: int = 50, overlap: int = 10) -> List[str]:
    """
    Разбивает текст на перекрывающиеся чанки по словам.
    :param text: Исходный текст
    :param chunk_size: Кол-во слов в чанке
    :param overlap: Перекрытие - сколько слов должно быть в перекрытии между чанками
    """
    
    words = text.split().split(".").split(",").split("!").split("?").split(";").split(":").split("-").split("\n")
    chunks = []
    
    step = max(1, chunk_size - overlap)
    
    for i in range(0, len(words), step):
        chunk_words = words[i : i + chunk_size]
        if not chunk_words:
            break
        chunks.append(" ".join(chunk_words))
        
    return chunks


def get_embeddings(chunks: List[str]) -> np.ndarray:
    """
    Превращает текстовые чанки в векторы.
    :param chunks: Список строк
    :param model_name: Название модели
    :return: Массив векторов (numpy)
    """
    model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')
    
    embeddings = model.encode(chunks, convert_to_numpy=True, show_progress_bar=False)
    
    return embeddings


def cos_compare(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """
    Косинусное сравнение.
    :param vec1: Вектор 1
    :param vec2: Вектор 2
    :return: Метрика идентичности от 0 до 1 
    """
    score = cosine_similarity([vec1], [vec2])[0][0]
    return float(score)
