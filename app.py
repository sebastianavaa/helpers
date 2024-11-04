import streamlit as st
from etl_script import ejecutar_etl
from google_sheet_helper import obtener_rut_por_empresa, obtener_lista_empresas
from datetime import date, datetime
import calendar

# Configuración de parámetros y token de acceso
token = st.secrets["TOKEN"]

# Título y descripción de la aplicación con emojis para un toque visual
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
        rut_empresa = obtener_rut_por_empresa_cached(nombre_empresa)
        
        if rut_empresa:
            st.write(f"✅ RUT encontrado para '**{nombre_empresa}**': `{rut_empresa}`")
            st.write(f"📅 Generando reporte hasta el **{fecha_hasta}**.")

            with st.spinner("⏳ Procesando datos, por favor espera..."):
                # Ejecutar el ETL y recibir el archivo Excel en memoria
                excel_data = ejecutar_etl(token, rut_empresa, nombre_empresa, fecha_hasta, st)

            if excel_data:
                excel_filename = f"{nombre_empresa.replace(' ', '_')}_{anio}-{mes_numero:02d}.xlsx"
                
                st.write("📂 **Descarga tu archivo aquí:**")
                st.download_button(
                    "📊 Descargar Excel",
                    data=excel_data,
                    file_name=excel_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
                st.success("¡Proceso completado con éxito! 🎉")
                st.balloons()
                st.write("🤖 **¡Reporte listo para que brilles en tus análisis contables!**")
            else:
                st.error("No se generaron datos para consolidar. 🤔 Intenta con otro período.")
        else:
            st.warning(f"No se encontró ningún RUT para la empresa '{nombre_empresa}'. Verifica el nombre ingresado. 🔎")
    else:
        st.info("Por favor, selecciona una empresa. 🏢")
