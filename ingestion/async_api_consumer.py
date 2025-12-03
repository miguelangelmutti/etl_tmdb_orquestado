import asyncio
import aiohttp
import time
import json

class AsyncAPIConsumer:
    def __init__(self, max_concurrency, total_requests, url, api_key, headers):
        self.max_concurrency = max_concurrency
        self.total_requests = total_requests
        self.url = url
        self.api_key = api_key
        self.headers = headers
        self.semaphore = asyncio.Semaphore(max_concurrency)

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
                print(f"Error {response.status} en página {page_number}")
                return None

    async def bound_fetch_page(self, session, page_number):
        """
        Envuelve la petición con el semáforo para limitar concurrencia.
        """
        async with self.semaphore:
            # print(f"Descargando página {page_number}...") 
            return await self.fetch_page(session, page_number)

    async def run(self):
        start_time = time.time()
        
        # 1. Fase de EXPLORACIÓN: Obtener página 1 para saber el total
        # -----------------------------------------------------------
        async with aiohttp.ClientSession() as session:
            print("--- Consultando página 1 para obtener total de páginas ---")
            
            # Esta llamada la hacemos "sola" (sin gather) porque dependemos de ella
            first_page_data = await self.fetch_page(session, 1)
            
            if not first_page_data:
                print("Falló la obtención de la primera página. Abortando.")
                return

            # Imaginemos que la API devuelve 'total_pages'. 
            # (TMDb usa exactamente esta clave)
            total_pages = first_page_data.get('total_pages', 1)
            
            # Por seguridad para el ejemplo, limitemos a TOTAL_REQUESTS páginas si la API devuelve muchas
            pages_to_fetch = min(total_pages, self.total_requests) 
            
            print(f"Total encontrado en API: {total_pages}. Vamos a descargar hasta la {pages_to_fetch}.")

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
                
                print(f"--- Lanzando {len(tasks)} tareas en paralelo ---")
                
                # Esperamos a que todas terminen
                other_pages_data = await asyncio.gather(*tasks)
                
                # Agregamos los resultados nuevos a nuestra lista principal
                # Filtramos None por si hubo errores
                all_results.extend([p for p in other_pages_data if p is not None])

        end_time = time.time()
        print(f"--- Proceso terminado en {end_time - start_time:.2f} segundos ---")
        print(f"Páginas procesadas correctamente: {len(all_results)}")
        
        # Aquí procesarías 'all_results', que es una lista de JSONs (uno por página)
        with open("all_results.json", "w") as f:
            json.dump(all_results, f)
