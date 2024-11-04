import streamlit as st
from etl_script import ejecutar_etl
from google_sheet_helper import obtener_rut_por_empresa, obtener_lista_empresas
from datetime import date, datetime
import calendar

# Verificar la existencia de credenciales y token en `secrets.toml`
try:
    username_correcto = st.secrets["credentials"]["username"]
    password_correcto = st.secrets["credentials"]["password"]
    token = st.secrets["TOKEN"]
except KeyError:
    st.error("Error: Las credenciales o el token no están configurados correctamente en `secrets.toml`.")
    st.stop()

# CSS para personalizar el diseño
st.markdown("""
    <style>
    .main {
        background-color: #f5f7fa;
        padding: 2rem;
    }
    h1, h2, h3 {
        color: #ffffff;
        font-weight: bold;
    }
    .stTextInput > div > input {
        background-color: #eef2f8;
        color: #333;
    }
    .stButton>button {
        background-color: #1a73e8;
        color: #fff;
        border-radius: 5px;
        font-weight: bold;
        border: none;
    }
    .stButton>button:focus {
        outline: none;
        border: 2px solid #000;
    }
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

# Función para la autenticación
def autenticar():
    if "intento_autenticacion" not in st.session_state:
        st.session_state["intento_autenticacion"] = False

    if not st.session_state["intento_autenticacion"]:
        st.image("https://i.imgur.com/OtqHhPZ.png", width=200)  # Reemplaza el enlace con tu logo o imagen
        st.title("🔒 Acceso seguro")
        st.write("Por favor, ingresa tus credenciales para acceder a la aplicación.")
        
        # Entrada de usuario y contraseña
        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        
        # Botón para iniciar sesión
        if st.button("Iniciar sesión"):
            if username == username_correcto and password == password_correcto:
                st.session_state["autenticado"] = True
            else:
                st.error("Usuario o contraseña incorrectos.")
            st.session_state["intento_autenticacion"] = True  # Actualizar el intento de autenticación

# Verificar si el usuario ya está autenticado
if "autenticado" not in st.session_state:
    st.session_state["autenticado"] = False

if not st.session_state["autenticado"]:
    autenticar()
else:
    # Título y descripción de la aplicación
    st.markdown("<h1 style='text-align: center;'>📊 Genera tu Libro Mayor Accountfy</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; font-size:1.1em;'>🔍 Selecciona el nombre de la empresa y el período para generar el reporte contable completo</p>", unsafe_allow_html=True)

    # Dividir en columnas para una mejor organización
    col1, col2 = st.columns(2)

    with col1:
        # Lista de empresas
        st.markdown("### 🏢 Selección de Empresa")
        @st.cache_data
        def obtener_lista_empresas_cached():
            return obtener_lista_empresas()

        @st.cache_data
        def obtener_rut_por_empresa_cached(nombre_empresa):
            return obtener_rut_por_empresa(nombre_empresa)

        lista_empresas = obtener_lista_empresas_cached()
        nombre_empresa = st.selectbox("Selecciona la Empresa:", lista_empresas)

    with col2:
        # Selección de año y mes
        st.markdown("### 📆 Período de Reporte")
        anio = st.selectbox("Año:", range(date.today().year, date.today().year - 10, -1))
        mes = st.selectbox("Mes:", list(calendar.month_name)[1:])

    # Calcular el último día del mes seleccionado
    if mes and anio:
        mes_numero = list(calendar.month_name).index(mes)
        ultimo_dia = calendar.monthrange(anio, mes_numero)[1]
        fecha_hasta = datetime(anio, mes_numero, ultimo_dia).date()

    # Botón para ejecutar el ETL
    if st.button("🚀 Generar Reporte", key="generar_reporte"):
        if nombre_empresa:
            rut_empresa = obtener_rut_por_empresa_cached(nombre_empresa)
            
            if rut_empresa:
                # Mensaje de bienvenida
                st.success(f"RUT encontrado para '**{nombre_empresa}**': `{rut_empresa}`")
                st.write(f"Generando reporte hasta el **{fecha_hasta}**.")

                with st.spinner("Procesando datos, por favor espera..."):
                    json_data, excel_data = ejecutar_etl(token, rut_empresa, nombre_empresa, fecha_hasta, st)

                if json_data and excel_data:
                    json_filename = f"{nombre_empresa.replace(' ', '_')}_{anio}-{mes_numero:02d}.json"
                    excel_filename = f"{nombre_empresa.replace(' ', '_')}_{anio}-{mes_numero:02d}.xlsx"

                    # Sección de descarga con iconos
                    st.markdown("### 📂 Descarga de Archivos:")
                    st.download_button(
                        label="📥 Descargar JSON",
                        data=json_data,
                        file_name=json_filename,
                        mime="application/json"
                    )
                    st.download_button(
                        label="📊 Descargar Excel",
                        data=excel_data,
                        file_name=excel_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

                    st.success("Proceso completado con éxito 🎉")
                    st.balloons()
                else:
                    st.error("No se generaron datos para consolidar. Intenta con otro período.")
            else:
                st.warning(f"No se encontró ningún RUT para la empresa '{nombre_empresa}'. Verifica el nombre ingresado.")
        else:
            st.info("Por favor, selecciona una empresa.")
