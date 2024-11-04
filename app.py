import streamlit as st
from etl_script import ejecutar_etl, crear_excel_en_memoria
from google_sheet_helper import obtener_rut_por_empresa, obtener_lista_empresas
from datetime import date, datetime
import calendar

# Configuración de parámetros y token de acceso
token = st.secrets["TOKEN"]

# Título y descripción de la aplicación con emojis para un toque visual
st.title("📊 Genera tu Libro Mayor Accountfy")
st.write("🔍 **Selecciona el nombre de la empresa y el período para generar el reporte contable completo!**")

# Obtener la lista de empresas del Google Sheet
lista_empresas = obtener_lista_empresas()

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
        rut_empresa = obtener_rut_por_empresa(nombre_empresa)
        
        if rut_empresa:
            # Mensaje de bienvenida personalizado
            st.write(f"✅ RUT encontrado para '**{nombre_empresa}**': `{rut_empresa}`")
            st.write(f"📅 Generando reporte hasta el **{fecha_hasta}**.")

            # Spinner de carga mientras se ejecuta el ETL
            with st.spinner("⏳ Procesando datos, por favor espera..."):
                # Ejecutar el proceso ETL usando el RUT obtenido y el nombre de la empresa
                libro_mayor_datos = ejecutar_etl(token, rut_empresa, nombre_empresa, fecha_hasta, st)

            if libro_mayor_datos:
                # Generar archivo Excel en memoria
                archivo_excel = crear_excel_en_memoria(libro_mayor_datos)

                # Descargar Excel desde la memoria
                excel_filename = f"{nombre_empresa.replace(' ', '_')}_{anio}-{mes_numero:02d}.xlsx"
                st.download_button(
                    "📊 Descargar Excel",
                    data=archivo_excel,
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
