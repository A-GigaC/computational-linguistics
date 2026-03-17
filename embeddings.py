import numpy as np
from typing import List
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


def get_chunks(text: str, separator: str = ".") -> List[str]:
    """
    Разбивает текст на перекрывающиеся чанки по словам.
    :param text: Исходный текст
    :param chunk_size: Кол-во слов в чанке
    :param overlap: Перекрытие - сколько слов должно быть в перекрытии между чанками
    """
    
    chunks = text.split(separator)
        
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


chunks_1 = get_chunks("Привет, как дела? Это тестовый текст для проверки функции get_chunks. Он должен быть разбит на несколько частей с перекрытием. Я люблю программировать на Python! Зачем? Потому что это круто! Россия - великая страна. Это очень крутой день для тестирования кода. Надеюсь, все работает правильно. Удачи всем программистам!")
chunks_2 = get_chunks("Привет, как дела? Это тестовый текст для проверки моих функций. Он должен быть разбит на несколько частей. Я люблю программировать на Rust! Зачем? Потому что это круто! Россия - великая страна. Это ужасный крутой день для тестирования питон-кода. Не имею никаких надежд, что это работаетправильно. Удачи всем нашим.")

embeddings_1 = get_embeddings(chunks_1)
embeddings_2 = get_embeddings(chunks_2)

for i in range(min(len(embeddings_1), len(embeddings_2))):
    score = cos_compare(embeddings_1[i], embeddings_2[i])
    print(f"Сравнение чанков {i + 1}: {score:.4f}")