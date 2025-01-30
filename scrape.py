import os
import requests
import re
import csv
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime
from backoff import expo, on_exception
from requests.exceptions import RequestException

# Lista de URLs a analizar
URLS = [
    "https://www.idealista.com/alquiler-viviendas/sevilla/nervion/nervion/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/nervion/buhaira-huerta-del-rey/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/nervion/luis-montoto-santa-justa/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/nervion/gran-plaza-marques-de-pickman-ciudad-jardin/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/nervion/san-bernardo-avenida-de-cadiz/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/nervion/el-juncal-el-plantinar/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/nervion/la-florida/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/santa-cruz-alfalfa/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/arenal-museo-tetuan/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/encarnacion-las-setas/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/puerta-carmona-puerta-osario-amador-de-los-rios/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/alameda/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/feria/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/plaza-de-la-gavidia-san-lorenzo/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/san-vicente/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/san-julian/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/centro/puerta-de-la-carne-juderia/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/bellavista-jardines-de-hercules/bellavista/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/cerro-amate/cerro-del-aguila/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/sevilla-este/avenida-de-las-ciencias/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/sevilla-este/alcalde-l-urunuela-palacio-de-congresos/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/sevilla-este/emilio-lemos/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/parque-alcosa/parque-alcosa/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/los-remedios/asuncion-adolfo-suarez/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/los-remedios/plaza-de-cuba-republica-argentina/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/los-remedios/ramon-de-carranza-madre-rafols/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/los-remedios/parque-de-los-principes-calle-niebla/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/los-remedios/tablada/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/los-remedios/blas-infante/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/macarena/doctor-fedriani/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/macarena/parlamento-torneo/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/macarena/pio-xii/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/macarena/los-carteros-san-diego/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/macarena/villegas-los-principes/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/pino-montano/pino-montano/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/san-jeronimo/san-jeronimo/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/san-pablo/san-pablo/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/la-palmera-los-bermejales/bami-pineda/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/la-palmera-los-bermejales/palmas-altas/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/la-palmera-los-bermejales/reina-mercedes-heliopolis/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/la-palmera-los-bermejales/los-bermejales/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/la-palmera-los-bermejales/la-palmera-manuel-siurot/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/triana/calle-betis-pages-del-corro/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/triana/ronda-de-triana-patrocinio-turrunuelo/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/triana/lopez-de-gomara/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/triana/el-tardon/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/triana/barrio-leon/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/triana/isla-de-la-cartuja/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/valdezorras-el-gordillo/valdezorras-el-gordillo/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/cerro-amate/palmete-padre-pio-hacienda-san-antonio/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/cerro-amate/santa-aurelia/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/cerro-amate/los-pajaritos/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/cerro-amate/juan-xxiii/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/cerro-amate/rochelambert/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/cerro-amate/la-plata/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/prado-de-san-sebastian-felipe-ii-bueno-monreal/el-porvenir/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/prado-de-san-sebastian-felipe-ii-bueno-monreal/prado-de-san-sebastian-ramon-carande/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/prado-de-san-sebastian-felipe-ii-bueno-monreal/felipe-ii-bueno-monreal/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/triana/lopez-de-gomara/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/santa-clara/santa-clara/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/santa-justa-miraflores-cruz-roja/ctra-de-carmona-miraflores/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/santa-justa-miraflores-cruz-roja/arroyo-santa-justa/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/santa-justa-miraflores-cruz-roja/cruz-roja-capuchinos/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/santa-justa-miraflores-cruz-roja/la-salle-avd-manuel-del-valle-las-naciones/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/torreblanca/torreblanca/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/prado-de-san-sebastian-felipe-ii-bueno-monreal/tiro-de-linea/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/cerro-amate/amate/",
    "https://www.idealista.com/alquiler-viviendas/sevilla/cerro-amate/su-eminencia-la-oliva/"
]

BASE_URL = "https://web.archive.org"
CDX_API = "https://web.archive.org/cdx/search/cdx"

# Nombre del archivo CSV
CSV_FILENAME = "datos.csv"

# Función con reintentos y backoff para realizar peticiones HTTP
@on_exception(expo, RequestException, max_tries=5, max_time=60)
def realizar_peticion(url, params):
    resp = requests.get(url, params=params, timeout=20, headers={"User-Agent": "Python Wayback Scraper"})
    resp.raise_for_status()
    return resp

# 1. Cargar datos existentes (si el CSV ya existe) en memoria
datos_existentes = []
if os.path.exists(CSV_FILENAME):
    with open(CSV_FILENAME, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            datos_existentes.append(row)

# Creamos un set de claves únicas para detectar duplicados.
# Por ejemplo, la tupla (timestamp, archive_url) como clave única.
claves_existentes = set(
    (d["timestamp"], d["archive_url"])
    for d in datos_existentes
)

# 2. Seleccionar solo 5 URLs de manera aleatoria
num_urls_a_tomar = 5
urls_seleccionadas = random.sample(URLS, num_urls_a_tomar)  # Elige 5 al azar

nuevos_resultados = []

for url in urls_seleccionadas:
    url_parts = url.strip('/').split('/')
    distrito = url_parts[-2]
    barrio = url_parts[-1]

    params = {
        'url': url,
        'from': '2020',
        'to': '2025',
        'output': 'json',
        'fl': 'timestamp,original,statuscode',
        'filter': 'statuscode:200'
    }
    try:
        resp = realizar_peticion(CDX_API, params=params)
        data = resp.json()
    except RequestException as e:
        print(f"Error al acceder a la API para {url}: {e}")
        continue

    capturas = data[1:]  # la primera línea de la respuesta es la cabecera
    print(f"Encontradas {len(capturas)} capturas para {distrito}/{barrio}.")

    for row in capturas:
        timestamp = row[0]
        original = row[1]
        status = row[2]

        archive_url = f"{BASE_URL}/web/{timestamp}/{original}"
        timestamp_legible = datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")

        # Verificamos si esta captura (timestamp + archive_url) ya existe
        clave = (timestamp_legible, archive_url)
        if clave in claves_existentes:
            # Ya la tenemos, no la guardamos de nuevo
            continue

        # Descargamos la página archivada para extraer el precio medio (si existe)
        try:
            page_resp = realizar_peticion(archive_url, params={})
        except RequestException as e:
            print(f"Error al descargar {archive_url}: {e}")
            continue

        soup = BeautifulSoup(page_resp.text, 'html.parser')
        elemento_precio = soup.find('p', class_='items-average-price')
        if elemento_precio:
            texto_precio = elemento_precio.get_text(strip=True)
            match_num = re.search(r"(\d+,\d+)", texto_precio)
            precio = match_num.group(1) if match_num else None
        else:
            precio = None

        nuevos_resultados.append({
            'timestamp': timestamp_legible,
            'archive_url': archive_url,
            'status': status,
            'distrito': distrito,
            'barrio': barrio,
            'precio_medio': precio
        })

        # Añadimos la clave al set de claves existentes
        claves_existentes.add(clave)

        # Pausa entre descargas para no saturar (3 segundos)
        time.sleep(3)

# 3. Combinar los datos existentes con los nuevos
datos_finales = datos_existentes + nuevos_resultados

# 4. Guardar todo en el CSV (sobrescribiendo el archivo completo con la nueva información)
with open(CSV_FILENAME, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(
        f,
        fieldnames=['timestamp', 'archive_url', 'status', 'distrito', 'barrio', 'precio_medio']
    )
    writer.writeheader()
    writer.writerows(datos_finales)

print(f"Hecho. Archivo '{CSV_FILENAME}' actualizado con {len(nuevos_resultados)} nuevos registros (si los había).")
