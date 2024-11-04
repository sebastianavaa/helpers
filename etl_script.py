import os
import requests
import datetime
import json
import pandas as pd
import time
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import tempfile

# Función principal del ETL adaptada para Streamlit con mensajes temporales de progreso
def ejecutar_etl(token, rut_empresa, nombre_empresa, fecha_hasta, st):
    año_consultado = fecha_hasta.year
    fecha_inicio = datetime.datetime(año_consultado, 1, 1)
    fecha_hasta_dt = datetime.datetime(año_consultado, fecha_hasta.month, fecha_hasta.day)
    
    DESCARGAS_DIR = tempfile.gettempdir()
    datos_consolidados = []

    while fecha_inicio <= fecha_hasta_dt:
        libro_mayor = obtener_libro_mayor_por_mes(token, rut_empresa, fecha_inicio, nombre_empresa, st)
        if libro_mayor:
            df_mensual = pd.DataFrame(libro_mayor)
            datos_consolidados.append(df_mensual)
            st.info(f"📄 Datos del mes {fecha_inicio.strftime('%Y-%m')} cargados.")
            time.sleep(1)
        
        siguiente_mes = fecha_inicio.month % 12 + 1
        siguiente_año = fecha_inicio.year + (1 if siguiente_mes == 1 else 0)
        fecha_inicio = datetime.datetime(siguiente_año, siguiente_mes, 1)
    
    if datos_consolidados:
        df_final = pd.concat(datos_consolidados, ignore_index=True)
        RUTA_EXCEL_ANUAL = f"{DESCARGAS_DIR}/{nombre_empresa.replace(' ', '_')}_Anual_{año_consultado}.xlsx"
        df_final.to_excel(RUTA_EXCEL_ANUAL, index=False)
        return RUTA_EXCEL_ANUAL
    else:
        st.error("No se generaron datos para consolidar.")
        return None
 
# Función para obtener el libro mayor de un mes específico
def obtener_libro_mayor_por_mes(token, rut_empresa, fecha_inicio, nombre_empresa, st):
    fecha_fin_mes = (fecha_inicio + datetime.timedelta(days=32)).replace(day=1) - datetime.timedelta(days=1)
    session = requests.Session()
    
    # Configuración de reintentos
    retries = Retry(
        total=5,               # Menor número de reintentos
        backoff_factor=3,      # Tiempo de espera creciente entre reintentos
        status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    cuentas_coma_separadas, cuenta_nombre_dict = obtener_cuentas(session, token, rut_empresa)
    if not cuentas_coma_separadas:
        st.error("No se encontraron cuentas de nivel 4 en el plan de cuentas.")
        return []

    try:
        datos_cuenta = llamar_api_libro_mayor(
            session, token, rut_empresa, cuentas_coma_separadas, 
            fecha_inicio.strftime('%Y-%m-%d'), fecha_fin_mes.strftime('%Y-%m-%d')
        )
    except requests.exceptions.RequestException as e:
        st.error(f"Error al obtener datos de la API para {nombre_empresa}: {str(e)}.")
        return []

    libro_mayor_datos = []
    for asiento in datos_cuenta:
        cuenta_codigo_completo = asiento.get('cuenta', '')
        codigo_cuenta = cuenta_codigo_completo[:10]
        nombre_cuenta = cuenta_codigo_completo[10:].strip()
        detalles = asiento.get('detalles', '')[:120].lower()
        
        if "apertura" not in detalles:
            diferencia = asiento['credito'] - asiento['debito']
            tipo = "D" if diferencia < 0 else "C"
            libro_mayor_datos.append({
                "Código de Cuenta": codigo_cuenta,
                "Cuenta": nombre_cuenta,
                "Crédito - Débito": diferencia,
                "Tipo": tipo,
                "Detalles": detalles,
                "Fecha de Contabilización": asiento.get('fecha_contabilizacion_humana', ''),
                "Centro de Costo": "",
                "Empresa": nombre_empresa,
                "Información Adicional": f"Asiento {asiento.get('numero_asiento', '')}",
                "Contraparte": asiento.get('contraparte', '')
            })
    
    return libro_mayor_datos

# Consolidación de archivos JSON en una lista única
def consolidar_archivos_json_como_lista(archivos_mensuales, ruta_archivo_anual):
    datos_consolidados = []
    for archivo in archivos_mensuales:
        with open(archivo, 'r') as json_file:
            datos = json.load(json_file)
            datos_consolidados.extend(datos)
    
    with open(ruta_archivo_anual, 'w') as archivo_anual:
        json.dump(datos_consolidados, archivo_anual, indent=4)
    print(f"Archivo JSON anual consolidado en: {ruta_archivo_anual}")

# Creación de archivo Excel desde JSON consolidado en lotes
def crear_excel_desde_json_en_lotes(ruta_json, ruta_excel=None):
    # Cargar los datos del archivo JSON
    with open(ruta_json, 'r') as json_file:
        datos = json.load(json_file)
        
    # Convertir los datos en un DataFrame
    df = pd.DataFrame(datos)

    # Usar un archivo temporal si no se proporciona ruta_excel
    if ruta_excel is None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_excel:
            ruta_excel = temp_excel.name
    
    # Exportar el DataFrame a Excel
    df.to_excel(ruta_excel, index=False)
    print(f"Archivo Excel anual creado en: {ruta_excel}")
    
    return ruta_excel


# Función para guardar en JSON con verificación adicional del directorio
def guardar_en_json(libro_mayor_datos, ruta_archivo=None):
    if ruta_archivo is None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_json:
            json.dump(libro_mayor_datos, temp_json, indent=4)
            print(f"Archivo JSON guardado en: {temp_json.name}")
        return temp_json.name
    else:
        with open(ruta_archivo, 'w') as json_file:
            json.dump(libro_mayor_datos, json_file, indent=4)
        print(f"Archivo JSON guardado en: {ruta_archivo}")

# Función para obtener el plan de cuentas desde la API
def obtener_cuentas(session, token, rut_empresa):
    url_plan_cuentas = f"https://api.clay.cl/v1/contabilidad/plan_cuenta/?rut_empresa={rut_empresa}"
    headers = {"Token": token, "accept": "application/json"}
    response = session.get(url_plan_cuentas, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        cuentas_nivel4 = [item['codigo'] for item in data.get('data', {}).get('items', []) if item.get('nivel') == 4]
        cuentas_coma_separadas = ','.join(cuentas_nivel4)
        return cuentas_coma_separadas, {item['codigo']: item['nombre'] for item in data.get('data', {}).get('items', []) if item.get('nivel') == 4}
    else:
        print(f"Error al obtener el plan de cuentas: {response.status_code}")
        return None, None

# Función para llamar a la API de libro mayor
def llamar_api_libro_mayor(session, token, rut_empresa, cuentas, fecha_desde, fecha_hasta):
    offset = 0
    limit = 100
    has_more_data = True
    datos_cuenta = []
    while has_more_data:
        url_libro_mayor = f"https://api.clay.cl/v1/contabilidad/libro_mayor/?tipo=tributario&ordenar_por=fecha_contabilizacion&cuenta={cuentas}&rut_empresa={rut_empresa}&fecha_desde={fecha_desde}&fecha_hasta={fecha_hasta}&limit={limit}&offset={offset}"
        headers = {"Token": token, "accept": "application/json"}
        response = session.get(url_libro_mayor, headers=headers)
        if response.status_code == 200:
            data = response.json()
            items = data.get('data', {}).get('items', [])
            if items:
                datos_cuenta.extend(items)
                offset += limit
            else:
                has_more_data = False
        else:
            print(f"Error al obtener el libro mayor: {response.status_code}")
            has_more_data = False
    return datos_cuenta
