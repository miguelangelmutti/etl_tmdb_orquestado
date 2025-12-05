import asyncio
import aiohttp
import time
import json
from utils.logger import setup_logger

class AsyncAPIConsumer:
    def __init__(self, max_concurrency, total_requests, url, api_key, headers):
        self.max_concurrency = max_concurrency
        self.total_requests = total_requests
        self.url = url
        self.api_key = api_key
        self.headers = headers
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.logger = setup_logger(__name__)

    async def fetch_page(self, session, page_number):
        """
        Descarga una página específica.
        """
        params = {
            'api_key': self.api_key,
            'page': page_number,
            'language': 'es-ES'
        }
        # Nota: Pasamos 'params' a session.get para que aiohttp arme la URL correctamente
        async with session.get(self.url, headers=self.headers, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                self.logger.error(f"Error {response.status} en página {page_number}")
                return None

    async def bound_fetch_page(self, session, page_number):
        """
        Envuelve la petición con el semáforo para limitar concurrencia.
        """
        async with self.semaphore:
            # print(f"Descargando página {page_number}...") 
            return await self.fetch_page(session, page_number)

    async def fetch_item(self, session, item_id, url_template):
        """
        Descarga un item específico por ID.
        """
        url = url_template.format(item_id)
        # params = {'api_key': self.api_key, 'language': 'es-ES'} # Opcional: si se requieren params comunes
        # Para movie details, api_key suele ir en query params también
        params = {
            'api_key': self.api_key,
            'language': 'es-ES'
        }
        
        async with session.get(url, headers=self.headers, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                self.logger.error(f"Error {response.status} en item {item_id}")
                return None

    async def bound_fetch_item(self, session, item_id, url_template):
        """
        Envuelve la petición de item con el semáforo.
        """
        async with self.semaphore:
            return await self.fetch_item(session, item_id, url_template)

    async def fetch_by_ids(self, ids: list, url_template: str):
        """
        Descarga en paralelo una lista de items dados sus IDs.
        """
        start_time = time.time()
        self.logger.info(f"--- Iniciando descarga de {len(ids)} items por ID ---")
        
        results = []
        async with aiohttp.ClientSession() as session:
            tasks = []
            for item_id in ids:
                task = self.bound_fetch_item(session, item_id, url_template)
                tasks.append(task)
            
            self.logger.info(f"--- Lanzando {len(tasks)} tareas en paralelo ---")
            responses = await asyncio.gather(*tasks)
            
            # Filtrar nulos
            results = [r for r in responses if r is not None]
            
            # Guardar resultados
            with open("changed_movies_results.json", "w") as f:
                json.dump(results, f)

        end_time = time.time()
        self.logger.info(f"--- Proceso de IDs terminado en {end_time - start_time:.2f} segundos ---")
        self.logger.info(f"Items procesados correctamente: {len(results)}")
        return results

    async def run(self):
        start_time = time.time()
        
        # 1. Fase de EXPLORACIÓN: Obtener página 1 para saber el total
        # -----------------------------------------------------------
        async with aiohttp.ClientSession() as session:
            self.logger.info("--- Consultando página 1 para obtener total de páginas ---")
            
            # Esta llamada la hacemos "sola" (sin gather) porque dependemos de ella
            first_page_data = await self.fetch_page(session, 1)
            
            if not first_page_data:
                self.logger.error("Falló la obtención de la primera página. Abortando.")
                return

            # Imaginemos que la API devuelve 'total_pages'. 
            # (TMDb usa exactamente esta clave)
            pages_to_fetch = first_page_data.get('total_pages', 1)
            self.logger.info(f"Total de páginas encontradas en API: {pages_to_fetch}. ")        
                           
            # Inicializamos la lista de resultados con los datos de la página 1
            all_results = [first_page_data]

            # 2. Fase de CONQUISTA: Paralelizar el resto
            # -----------------------------------------------------------
            if pages_to_fetch > 1:
                tasks = []
                
                # Iteramos desde la página 2 hasta la N
                for page_num in range(2, pages_to_fetch + 1):
                    task = self.bound_fetch_page(session, page_num)
                    tasks.append(task)
                
                self.logger.info(f"--- Lanzando {len(tasks)} tareas en paralelo ---")
                
                # Esperamos a que todas terminen
                other_pages_data = await asyncio.gather(*tasks)
                
                # Agregamos los resultados nuevos a nuestra lista principal
                # Filtramos None por si hubo errores
                all_results.extend([p for p in other_pages_data if p is not None])

        end_time = time.time()
        self.logger.info(f"--- Proceso terminado en {end_time - start_time:.2f} segundos ---")
        self.logger.info(f"Páginas procesadas correctamente: {len(all_results)}")
        
        # Aquí procesarías 'all_results', que es una lista de JSONs (uno por página)
        with open("all_changes_results.json", "w") as f:
            json.dump(all_results, f)
