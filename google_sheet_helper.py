import gspread
from google.oauth2.service_account import Credentials

# Configuración de autenticación
ruta_credenciales = "/Users/sebastiannava/Desktop/ProyectosGithub/helpers/our-metric-440014-f5-3e6296bfc0aa.json"  # Cambia esto por la ruta a tu archivo de credenciales
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(ruta_credenciales, scopes=scope)
client = gspread.authorize(creds)

# URL del Google Sheet (cambia esto por el URL de tu documento)
spreadsheet_url = "https://docs.google.com/spreadsheets/d/10q30KbEvFlyIYqPebkQGwQHhdaf6epPrx1uuROPR3eA/edit?gid=1662715647#gid=1662715647"

# Función para obtener la hoja de cálculo (spreadsheet) y una hoja (worksheet) específica
def obtener_hoja(hoja_nombre="Accountfy Nombres"):
    """Obtiene la hoja específica de un documento de Google Sheets."""
    sheet = client.open_by_url(spreadsheet_url)
    worksheet = sheet.worksheet(hoja_nombre)
    return worksheet

# Función para obtener el RUT a partir del nombre de la empresa
def obtener_rut_por_empresa(nombre_empresa, hoja_nombre="Accountfy Nombres"):
    """
    Busca el RUT correspondiente a un nombre de empresa en la hoja especificada.
    Retorna el RUT si encuentra la empresa, o None si no hay coincidencia.
    """
    worksheet = obtener_hoja(hoja_nombre)
    datos = worksheet.get_all_values()
    
    # Obtener los índices de las columnas por encabezado
    encabezados = datos[0]
    try:
        indice_rut = encabezados.index("Rut sin guión")
        indice_empresa = encabezados.index("Empresa Accountfy")
    except ValueError:
        raise ValueError("Asegúrate de que los encabezados 'Rut sin guión' y 'Empresa Accountfy' existen en la hoja")

    # Buscar la fila que coincide con el nombre de la empresa
    for fila in datos[1:]:  # Omitir encabezados
        if fila[indice_empresa].strip().lower() == nombre_empresa.strip().lower():
            return fila[indice_rut]  # Retorna el RUT si encuentra la empresa
    return None  # Devuelve None si no se encuentra

# Función para obtener información de la empresa a partir del RUT
def obtener_datos_empresa(rut_buscado, hoja_nombre="Accountfy Nombres"):
    """
    Busca el nombre de la empresa a partir del RUT en la hoja especificada.
    Retorna el nombre de la empresa si encuentra el RUT, o None si no hay coincidencia.
    """
    worksheet = obtener_hoja(hoja_nombre)
    datos = worksheet.get_all_values()
    
    # Obtener los índices de las columnas por encabezado
    encabezados = datos[0]
    try:
        indice_rut = encabezados.index("Rut sin guión")
        indice_empresa = encabezados.index("Empresa Accountfy")
    except ValueError:
        raise ValueError("Asegúrate de que los encabezados 'Rut sin guión' y 'Empresa Accountfy' existen en la hoja")

    # Buscar la fila que coincide con el RUT
    for fila in datos[1:]:  # Omitir encabezados
        if fila[indice_rut] == rut_buscado:
            return fila[indice_empresa]  # Retorna el nombre de la empresa si encuentra el RUT
    return None  # Devuelve None si no se encuentra

# Función para obtener todos los valores de una fila específica (por índice de fila)
def obtener_fila_por_indice(indice_fila, hoja_nombre="Accountfy Nombres"):
    """
    Obtiene todos los valores de una fila específica según su índice.
    """
    worksheet = obtener_hoja(hoja_nombre)
    try:
        fila = worksheet.row_values(indice_fila)
        return fila
    except gspread.exceptions.APIError:
        raise ValueError(f"No se pudo encontrar la fila {indice_fila} en la hoja '{hoja_nombre}'")

# Función para obtener todos los datos en la hoja en forma de lista de diccionarios
def obtener_todos_los_datos(hoja_nombre="Accountfy Nombres"):
    """
    Obtiene todos los datos de la hoja como una lista de diccionarios.
    """
    worksheet = obtener_hoja(hoja_nombre)
    datos = worksheet.get_all_records()  # Retorna todos los datos como lista de diccionarios
    return datos

# Nueva función para obtener la lista de nombres de empresas
def obtener_lista_empresas(hoja_nombre="Accountfy Nombres"):
    """
    Obtiene una lista de todos los nombres de empresas en la columna 'Empresa Accountfy'.
    """
    worksheet = obtener_hoja(hoja_nombre)
    datos = worksheet.get_all_values()
    
    encabezados = datos[0]
    try:
        indice_empresa = encabezados.index("Empresa Accountfy")
    except ValueError:
        raise ValueError("Asegúrate de que el encabezado 'Empresa Accountfy' existe en la hoja")

    # Extraer todos los nombres de empresas y eliminar duplicados
    lista_empresas = sorted(set(fila[indice_empresa] for fila in datos[1:] if fila[indice_empresa]))
    return lista_empresas
