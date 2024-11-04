import streamlit as st
from etl_script import ejecutar_etl
from google_sheet_helper import obtener_rut_por_empresa, obtener_lista_empresas
from datetime import date, datetime
import calendar

# Configuración de usuario y contraseña desde el archivo de configuración
username_correcto = st.secrets["credentials"]["username"]
password_correcto = st.secrets["credentials"]["password"]

# Función para la autenticación
def autenticar():
    st.title("🔒 Acceso seguro")
    st.write("Por favor, ingresa tus credenciales para acceder a la aplicación.")
    
    # Entrada de usuario y contraseña
    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")
    
    # Botón para iniciar sesión
    if st.button("Iniciar sesión"):
        if username == username_correcto and password == password_correcto:
            st.session_state["autenticado"] = True
            st.success("¡Acceso concedido!")
            st.experimental_rerun()  # Recargar la página para mostrar la app
        else:
            st.error("Usuario o contraseña incorrectos.")

# Verificar si el usuario ya está autenticado
if "autenticado" not in st.session_state:
    st.session_state["autenticado"] = False

if not st.session_state["autenticado"]:
    autenticar()
else:
    # Configuración de parámetros y token de acceso
    token = st.secrets["TOKEN"]

    # Título y descripción de la aplicación
    st.title("📊 Genera tu Libro Mayor Accountfy")
    st.write("🔍 **Selecciona el nombre de la empresa y el período para generar el reporte contable completo!**")

    # Caching para reducir llamadas a Google Sheets
    @st.cache_data
    def obtener_lista_empresas_cached():
        return obtener_lista_empresas()

    @st.cache_data
    def obtener_rut_por_empresa_cached(nombre_empresa):
        return obtener_rut_por_empresa(nombre_empresa)

    # Obtener la lista de empresas del Google Sheet usando caché
    lista_empresas = obtener_lista_empresas_cached()

    # Mostrar el menú desplegable con los nombres de las empresas
    nombre_empresa = st.selectbox("🏢 Selecciona la Empresa:", lista_empresas)

    # Selección de año y mes
    anio = st.selectbox("📅 Selecciona el Año:", range(date.today().year, date.today().year - 10, -1))
    mes = st.selectbox("📆 Selecciona el Mes:", list(calendar.month_name)[1:])

    # Calcular el último día del mes seleccionado
    if mes and anio:
        mes_numero = list(calendar.month_name).index(mes)
        ultimo_dia = calendar.monthrange(anio, mes_numero)[1]
        fecha_hasta = datetime(anio, mes_numero, ultimo_dia).date()

    # Botón para ejecutar el ETL
    if st.button("🚀 Ejecutar ETL"):
        if nombre_empresa:
            # Obtener el RUT de la empresa seleccionada usando la función cacheada
            rut_empresa = obtener_rut_por_empresa_cached(nombre_empresa)
            
            if rut_empresa:
                # Mensaje de bienvenida personalizado
                st.write(f"✅ RUT encontrado para '**{nombre_empresa}**': `{rut_empresa}`")
                st.write(f"📅 Generando reporte hasta el **{fecha_hasta}**.")

                # Spinner de carga mientras se ejecuta el ETL
                with st.spinner("⏳ Procesando datos, por favor espera..."):
                    # Ejecutar el proceso ETL usando el RUT obtenido y el nombre de la empresa
                    json_data, excel_data = ejecutar_etl(token, rut_empresa, nombre_empresa, fecha_hasta, st)

                if json_data and excel_data:
                    # Nombres personalizados para los archivos de descarga
                    json_filename = f"{nombre_empresa.replace(' ', '_')}_{anio}-{mes_numero:02d}.json"
                    excel_filename = f"{nombre_empresa.replace(' ', '_')}_{anio}-{mes_numero:02d}.xlsx"

                    st.write("📂 **Descarga tus archivos aquí:**")
                    # Descargar JSON desde la memoria
                    st.download_button(
                        label="📥 Descargar JSON",
                        data=json_data,
                        file_name=json_filename,
                        mime="application/json"
                    )
                    
                    # Descargar Excel desde la memoria
                    st.download_button(
                        label="📊 Descargar Excel",
                        data=excel_data,
                        file_name=excel_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    # Efecto visual de finalización
                    st.success("¡Proceso completado con éxito! 🎉")
                    st.balloons()
                    
                    # Frase de despedida divertida
                    st.write("🤖 **¡Reporte listo para que brilles en tus análisis contables!**")
                else:
                    st.error("No se generaron datos para consolidar. 🤔 Intenta con otro período.")
            else:
                st.warning(f"No se encontró ningún RUT para la empresa '{nombre_empresa}'. Verifica el nombre ingresado. 🔎")
        else:
            st.info("Por favor, selecciona una empresa. 🏢")
