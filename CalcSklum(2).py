#!/usr/bin/env python
# coding: utf-8

# In[23]:
import pandas as pd
import streamlit as st
import sqlite3
from io import BytesIO
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pyodbc
from sqlalchemy import create_engine
import urllib
# In[25]:
import streamlit as st

uploaded_file= "VD_HERRAMIENTA POLÍTICA RETRIBUTIVA_GRUPO 3D SOLUTIONS.xlsx"
# In[27]:

maestroPersonas= pd.read_excel(uploaded_file, sheet_name='Maestro personas')
PuestoPreg = pd.read_excel(uploaded_file, sheet_name='Puesto-Preguntas')
#Resuls = pd.read_excel(uploaded_file, sheet_name='Resultados Objetivo')
archivo_valoraciones= "archivo_valoraciones.csv"
t33 = pd.read_excel(uploaded_file, sheet_name='Tabla3.3')
t4 = pd.read_excel(uploaded_file, sheet_name='TABLA 4')
t2 = pd.read_excel(uploaded_file, sheet_name='TABLA 2')
dfContras = maestroPersonas[["SUPERVISOR", "Director_Área","Contraseña"]]
file_pathResuls= "ResultadosRRHH.xlsx"
file_pathVals= "ValsFusionado_sin_duplicados.xlsx"
df_valoraciones = pd.read_excel(file_pathVals)
df_resultados_nuevos = pd.read_excel(file_pathResuls)
df_valoraciones['Fecha'] = pd.to_datetime(df_valoraciones['Fecha'], errors='coerce')
df_resultados_nuevos['Fecha'] = pd.to_datetime(df_resultados_nuevos['Fecha'], errors='coerce')

df_valoraciones['Fecha'] = df_valoraciones['Fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')
df_resultados_nuevos['Fecha'] = df_resultados_nuevos['Fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')

# In[33]:
def conectar_db():
    conn = sqlite3.connect('retribuciones67.db')
    return conn


def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Hoja1')
    output.seek(0)
    return output
    
def apply_filters(df, area,  Evaluador, Puesto, Nombre):
    if area != 'Todos':
        df = df[df['Área'] == area]
    if Evaluador != 'Todos':
        df = df[df['Evaluador'] == Evaluador]
    if Puesto != 'Todos':
        df = df[df['Puesto'] == Puesto]
    if Nombre != 'Todos':
        df = df[df['Nombre'] == Nombre]
    return df

def eliminar_todas_las_tablas():
    conn = sqlite3.connect('retribuciones67.db')
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence';")
    tablas = cursor.fetchall()

    for tabla in tablas:
        Nombre_tabla = tabla[0]
        cursor.execute(f"DROP TABLE IF EXISTS {Nombre_tabla}")

    conn.commit()
    conn.close()

#eliminar_todas_las_tablas()

def vaciar_bd_retribuciones2():
    conn = sqlite3.connect('retribuciones67.db')
    cursor = conn.cursor()

    # Eliminar todos los registros de la tabla retribuciones2
    cursor.execute('DELETE FROM retribuciones2')
    cursor.execute('DELETE FROM valoraciones')

    # Confirmar cambios y cerrar la conexión
    conn.commit()
    conn.close()

# Llamar a la función para vaciar la base de datos
#vaciar_bd_retribuciones2()

def ver_datos():
    conn= sqlite3.connect('retribuciones67.db')
    query = "SELECT * FROM valoraciones"
    dfvaloraciones = pd.read_sql(query, conn)
    conn.close()
    return dfvaloraciones

def ver_datos2():
    conn= sqlite3.connect('retribuciones67.db')
    query = "SELECT * FROM retribuciones2"
    dfretribuciones2 = pd.read_sql(query, conn)
    conn.close()
    return dfretribuciones2

def crear_tablas():
    conn = sqlite3.connect('retribuciones67.db')  # Asegúrate de que es la base correcta
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS valoraciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Evaluador TEXT NOT NULL,
            Nombre TEXT NOT NULL,
            Área TEXT NOT NULL,
            Puesto TEXT NOT NULL,
            Departamento TEXT NOT NULL,
            Empresa TEXT,
            Ponderación TEXT NOT NULL,
            Sección TEXT,
            ItinerarioNivel TEXT NOT NULL,
            idConocimiento INTEGER NOT NULL,
            Conocimiento TEXT NOT NULL,
            TipoConocimientos TEXT NOT NULL,
            Valoración INTEGER NOT NULL,
            Fecha TEXT NOT NULL
        )

    ''')
          
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS retribuciones2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Evaluador TEXT NOT NULL,
    Nombre TEXT NOT NULL,
    Área TEXT NOT NULL,
    Puesto TEXT NOT NULL,
    Departamento TEXT,
    Sección TEXT,
    Empresa TEXT,
    Ponderación TEXT NOT NULL,
    Nivel TEXT NOT NULL,
    ItinerarioNivel TEXT NOT NULL,
    Valoración_Obtenida INTEGER NOT NULL,
    Retribución_Actual REAL,
    RetrVariable_Actual REAL,
    Retr_Propuesta REAL,
    RetrVariable_Propuesta REAL,
    Diferencia_Retr REAL,
    Observación TEXT,
    Fecha TEXT NOT NULL
)
    ''')
    
    conn.commit()
    conn.close()
crear_tablas()

def insertar_valoraciones_en_sql(df_valoraciones_actualizadas):
    conn = sqlite3.connect('retribuciones67.db')
    cursor = conn.cursor()

    for _, row in df_valoraciones_actualizadas.iterrows():
        cursor.execute('''
            INSERT INTO valoraciones (
                Evaluador,
                Nombre,
                Área,
                Puesto,
                Departamento,
                Sección,
                Empresa,
                Ponderación,
                ItinerarioNivel,
                Conocimiento,
                Conocimiento,
                TipoConocimientos,
                Valoración,
                Fecha
            ) VALUES (?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['Evaluador'],  
            row['Nombre'],
            row['Área'],
            row['Puesto'],
            row['Departamento'],
            row['Sección'],
            row['Empresa'],
            row['Ponderación'],
            row['ItinerarioNivel'],
            row['idConocimiento'],
            row['Conocimiento'],
            row['TipoConocimientos'],
            row['Valoración'],
            row['Fecha']
        ))
    conn.commit()
    conn.close()
def insertar_resultados_en_sql(df_resultados):
    conn = sqlite3.connect('retribuciones67.db')
    cursor = conn.cursor()

    for _, row in df_resultados.iterrows():
        cursor.execute('''
            INSERT INTO retribuciones2 (
                Evaluador,
                Nombre,
                Área,
                Puesto,
                Departamento,
                Sección,
                Empresa,
                Ponderación,
                Nivel,
                ItinerarioNivel,
                Valoración_Obtenida, 
                Retribución_Actual,
                RetrVariable_Actual, 
                Retr_Propuesta,
                RetrVariable_Propuesta, 
                Diferencia_Retr,
                Observación,
                Fecha
            ) VALUES (?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['Evaluador'],  
            row['Nombre'],
            row['Área'],    
            row['Puesto'],
            row['Departamento'],
            row['Sección'],
            row['Empresa'],
            row['Ponderación'],
            row['Nivel'],
            row['ItinerarioNivel'],
            row['Valoración_Obtenida'],   
            row['Retribución_Actual'],
            row['RetrVariable_Actual'],   
            row['Retr_Propuesta'],
            row['RetrVariable_Propuesta'],   
            row['Diferencia_Retr'],
            row['Observación'],
            row['Fecha'],
        ))

    conn.commit()
    conn.close()

def show_logoImp():
    st.image("logoImproven.png", width=150)
def show_logoSk():
    st.image("logoSklum.png", width=250)
def highlight_cells(val):
    if val > 0:
        color = 'green'
    elif val < 0:
        color = 'red'
    else:
        return ''  # No aplicar ningún estilo si es 0
    return f'color: {color}'
def formato_euro(val):
    val = str(val).replace(",", "")
    val = float(val)
    return f"{val:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")
def autenticar_usuario(usuario, contraseña):
    return diccUsu_Contra.get(usuario) == contraseña
st.set_page_config(page_title="", layout="wide", initial_sidebar_state="collapsed")

def insertar_nuevas_valoracionesExcel(df, table_name, unique_columns):
    """
    Inserta en la base de datos solo los registros que no existen ya.
    Parameters:
        df (DataFrame): DataFrame con los nuevos datos
        table_name (str): Nombre de la tabla en la base de datos
        unique_columns (list): Lista de columnas que identifican un registro único
    """
    conn = sqlite3.connect('retribuciones67.db')
    cursor = conn.cursor()
    conditions = " AND ".join([f"{col} = ?" for col in unique_columns])
    check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {conditions}"
 
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
 
    for _, row in df.iterrows():
        values = tuple(row[col] for col in unique_columns)
 
        cursor.execute(check_query, values)
        exists = cursor.fetchone()[0]
 
        if exists == 0:  # Solo insertar si no existe
            cursor.execute(insert_query, tuple(row))
 
    conn.commit()
    conn.close()
# Insertar nuevas valoraciones en la tabla "valoraciones"

def insertar_nuevos_resultados(df, table_name, unique_columns):
    """
    Inserta en la base de datos solo los registros que no existen ya.
    Parameters:
        df (DataFrame): DataFrame con los nuevos datos
        table_name (str): Nombre de la tabla en la base de datos
        unique_columns (list): Lista de columnas que identifican un registro único
    """
    conn = sqlite3.connect('retribuciones67.db')
    cursor = conn.cursor()
    conditions = " AND ".join([f"{col} = ?" for col in unique_columns])
    check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {conditions}"
 
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
 
    for _, row in df.iterrows():
        values = tuple(row[col] for col in unique_columns)
 
        cursor.execute(check_query, values)
        exists = cursor.fetchone()[0]
 
        if exists == 0:  # Solo insertar si no existe
            cursor.execute(insert_query, tuple(row))
 
    conn.commit()
    conn.close() 
# Insertar nuevos resultados en la tabla "retribuciones2"
 
opciones_respuestas = {
    "Responsabilidades y funciones": {
        "No aplica.": 0,
        "Responsabilidad desarrollada bajo alto grado de supervisión.": 1,
        "Responsabilidad desarrollada de manera autónoma pero con supervisión ocasional.": 2,
        "Responsabilidad consolidada con grado alto de autonomía.": 3
    },
    "Conocimientos funcionales": {
        "No tiene Conocimiento.": 0,
        "Conocimientos básicos.": 1,
        "Conocimientos en desarrollo.": 2,
        "Conocimientos consolidados.": 3,
        "Conocimientos consolidados, siendo capaz de formar.": 4,
        "Conocimientos consolidados, siendo capaz de formar e implementar mejoras.": 5
    },
    "Competencias": {
        "Nunca": 0,
        "Casi nunca": 1,
        "A veces": 2,
        "Casi siempre": 3,
        "Siempre": 4
    }
}
ponderaciones = {
    "Ponderación 1": {"Responsabilidades y funciones": 0.30, "Conocimientos funcionales": 0.25, "Competencias": 0.45},
    "Ponderación 2": {"Responsabilidades y funciones": 0.30, "Conocimientos funcionales": 0.30, "Competencias": 0.40},
    "Ponderación 3": {"Responsabilidades y funciones": 0.35, "Conocimientos funcionales": 0.30, "Competencias": 0.35},
    "Ponderación 4": {"Responsabilidades y funciones": 0.40, "Conocimientos funcionales": 0.30, "Competencias": 0.30},
    "Ponderación 5": {"Responsabilidades y funciones": 0.45, "Conocimientos funcionales": 0.30, "Competencias": 0.25},
    "Ponderación 6": {"Responsabilidades y funciones": 0.55, "Conocimientos funcionales": 0.20, "Competencias": 0.25},
}

# Definir diccionario de usuarios y contraseñas
diccUsu_Contra = pd.Series(dfContras["Contraseña"].values, index=dfContras["SUPERVISOR"]).to_dict()
insertar_nuevas_valoracionesExcel(df_valoraciones, "valoraciones", ["Evaluador", "Nombre","Conocimiento", "Fecha"])
insertar_nuevos_resultados(df_resultados_nuevos, "retribuciones2", ["Evaluador", "Nombre", "Fecha"])
# Inicializar estado de autenticación
if 'authenticated' not in st.session_state:
    st.title('PROCESO DE EVALUACIÓN DEL DESEMPEÑO')
    st.session_state.authenticated = False
    st.session_state.user = None  # Almacena el Nombre del usuario autenticado

# Si está autenticado, continuar con el flujo de la aplicación
if st.session_state.authenticated:


    df_personas = maestroPersonas
    df_Puesto_pregs = PuestoPreg

    # Cargar valoraciones existentes
    if os.path.exists(archivo_valoraciones):
        df_valoraciones_existentes = pd.read_csv(archivo_valoraciones)
    else:
        df_valoraciones_existentes = pd.DataFrame()

    # Obtener el usuario autenticado
    usuario_autenticado = st.session_state.user

    # Filtrar por Evaluador o mostrar valoraciones si es administrador
    if usuario_autenticado not in ["admin", "admin2"]:
        # Verificar que la columna "SUPERVISOR" existe antes de filtrar
        if "SUPERVISOR" in df_personas.columns and "Área" in df_personas.columns:
            # Filtrar por usuario autenticado
            df_filtrado = df_personas[df_personas["SUPERVISOR"] == usuario_autenticado]
    
            # Verificar si hay coincidencias antes de acceder a "Área"
            if not df_filtrado.empty:
                area_usuario = df_filtrado["Área"].iloc[0]  # Obtener el primer valor válido
            else:
                area_usuario = None  # Valor predeterminado si no hay coincidencias
        else:
            area_usuario = None  

        if not df_filtrado.empty and "Director_Área" in df_filtrado.columns:
            direct = df_filtrado["Director_Área"].iloc[0]  # Usa .iloc[0] en lugar de [0] para evitar KeyError
        else:
            direct = "No"
        if direct=="No":
            lat = st.sidebar.selectbox('Menú',('Evaluar','Resultados', 'Resultados en detalle'))
            if lat=='Evaluar':
                st.title('PROCESO DE EVALUACIÓN DEL DESEMPEÑO')            
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)
                departamentos = df_filtrado["Departamento"].dropna().astype(str).unique()
                Departamento_seleccionado = st.selectbox(
                    'Selecciona el Departamento:',
                    ['Todos'] + sorted(departamentos),
                    label_visibility='collapsed')
    
                df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
        
                seccion_seleccionada = st.selectbox(
                    'Selecciona la Sección:',
                    ['Todos'] + sorted(df_filtrado['Sección'].astype(str).unique().tolist()),
                    label_visibility='collapsed'
                )
        
                df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
        
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
                Puesto_seleccionado = st.selectbox(
                    'Selecciona el Puesto:',
                    ['Todos'] + sorted(df_filtrado['Puesto'].unique().tolist()),
                    label_visibility='collapsed'
                )
                        
                df_valoraciones_actualizadas = ver_datos()
                df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)
                Empresa_seleccionada = st.selectbox(
                    'Selecciona la Empresa:',
                    ['Todos'] + sorted(df_filtrado['Empresa'].dropna().astype(str).unique().tolist()),
                    label_visibility='collapsed'
                )

                df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]
                filtro_evaluacion = st.radio("Filtrar personas:",
            ["No evaluadas", "Evaluadas"])
                personas_evaluadas = df_valoraciones_actualizadas["Nombre"].str.strip().unique().tolist()
                df_filtrado["Evaluada"] = df_filtrado["Nombre"].str.strip().isin(personas_evaluadas)
                if filtro_evaluacion == "No evaluadas":
                    df_filtrado = df_filtrado[~df_filtrado["Evaluada"]]
                else:
                    df_filtrado = df_filtrado[df_filtrado["Evaluada"]]
                
                
                    # Determinar la persona seleccionada
                Nombre_seleccionado = st.selectbox("Selecciona una persona a valorar:", df_filtrado["Nombre"].unique())
                if not df_filtrado.empty:
    
                    evaluada = Nombre_seleccionado.strip() in df_valoraciones_actualizadas["Nombre"].str.strip().values
                    if evaluada:
                        st.markdown('<p style="color:red;">Persona ya evaluada</p>', unsafe_allow_html=True)
            
                    persona = df_filtrado[df_filtrado["Nombre"] == Nombre_seleccionado].iloc[0]
                    area_persona = persona["Área"]
                    Puesto_persona = persona["Puesto"]
                    Departamento_persona = persona["Departamento"]
                    seccion_persona = persona["Sección"]
                    empresa_persona = persona["Empresa"]
                    ItinerarioNivel = persona["ItinerarioNivel"]
                    Ponderación = persona["Ponderación"]
                    st.write(f"Área: **{area_persona}** | Puesto: **{Puesto_persona}**")
            
                    Conocimientos = df_Puesto_pregs[(df_Puesto_pregs["Área"] == area_persona) & 
                                                    (df_Puesto_pregs["Puesto"] == Puesto_persona)]
                    
                    if not Conocimientos.empty:
                        st.markdown("**Instrucciones del cuestionario:**")
                        valoraciones = []
                        Fecha_actual = datetime.now()
            
                        st.markdown("""
                        En el presente cuestionario aparecen los criterios definidos (Responsabilidades, Conocimientos funcionales y Competencias) y validados en la ficha de desarrollo.<br><br>
                        El objetivo del cuestionario consiste en realizar la valoración de dichos criterios. Esta información nos será útil, solo si se responde de una manera sincera y objetiva.<br><br>
                        Gracias por tu colaboración.
                        """, unsafe_allow_html=True)
            
                        tipo_actual = None
                        
                        for i in range(len(Conocimientos)):
                            row = Conocimientos.iloc[i]
                            Conocimiento = row["Conocimiento"]
                            tipo_Conocimiento = row["TipoPreguntas"]
                            opciones = opciones_respuestas.get(tipo_Conocimiento, ["No disponible"])
                            idConocimiento = row["ID Conocimiento"]
            
                            # Asegurar que "Nombre" y "idConocimiento" sean del mismo tipo en df_valoraciones_actualizadas
                            df_valoraciones_actualizadas["Nombre"] = df_valoraciones_actualizadas["Nombre"].str.strip()
                            df_valoraciones_actualizadas["idConocimiento"] = df_valoraciones_actualizadas["idConocimiento"].astype(str)
                            idConocimiento = str(idConocimiento)  # Convertimos la variable al mismo tipo
                            
                            # Buscar la respuesta previa si la persona ya fue evaluada
                            if evaluada:
                                valoracion_previa = df_valoraciones_actualizadas.query(
                                    "Nombre == @Nombre_seleccionado and idConocimiento == @idConocimiento"
                                )["Valoración"]
                                
                                # Si hay una respuesta previa, tomarla; si no, None
                                if not valoracion_previa.empty:
                                    valoracion_seleccionada = valoracion_previa.iloc[0]  # Usa iloc en lugar de values[0]
                                else:
                                    valoracion_seleccionada = None
                            else:
                                valoracion_seleccionada = None
        
                            # Agregar título del tipo de Conocimiento si cambia
                            if tipo_Conocimiento != tipo_actual:
                                st.subheader(f"{tipo_Conocimiento}")
                                tipo_actual = tipo_Conocimiento
            
                            st.markdown(f"""
                                <div style="font-weight: bold; font-size: 18px; margin-bottom: -15px;">
                                    {Conocimiento}
                                </div>""", unsafe_allow_html=True)
                            
                            # Preseleccionar la respuesta previa en el radio button
                            valoracion = st.radio(
                                "", opciones, key=f"Conocimiento_{idConocimiento}", index=opciones.index(valoracion_seleccionada) if valoracion_seleccionada in opciones else 0
                            )
                            
            
                            valoraciones.append({
                                "Evaluador": usuario_autenticado,
                                "Nombre": Nombre_seleccionado,
                                "Área": area_persona,
                                "Puesto": Puesto_persona,
                                "Departamento": Departamento_persona,
                                "Sección": seccion_persona,
                                "Empresa": empresa_persona,
                                "Ponderación": Ponderación,
                                "ItinerarioNivel": ItinerarioNivel,
                                "idConocimiento": idConocimiento,
                                "Conocimiento": Conocimiento,
                                "TipoConocimientos": tipo_Conocimiento,
                                "Valoración": valoracion,
                                "Fecha": (Fecha_actual + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
                            })
        
        
                        observacion = st.text_area(
                            "Añadir observación (opcional):", 
                            key=f"Observacion_{idConocimiento}",
                            help="Puedes escribir comentarios adicionales sobre la evaluación.")    
                        if st.button("Guardar valoraciones"):
                            
                            df_nuevas_valoraciones = pd.DataFrame(valoraciones)
                            def ponderar_valoracion_puntu(row, *args, **kwargs):
                                puesto = row.get("Puesto", None)
                                tipo_pregunta = row.get("TipoConocimientos", None)  # Tomamos TipoPreguntas
                            
                                if puesto is None or tipo_pregunta is None:
                                    return 0  # Evita errores en caso de datos mal formateados
                            
                                # 🔹 Filtrar df_personas sin sobrescribirlo
                                df_filtrado = df_personas[df_personas['Puesto'] == puesto]
                            
                                if df_filtrado.empty:
                                    return 0  # Evita errores si el puesto no existe en df_personas
                            
                                # 🔹 Extraer la ponderación del puesto
                                ponderacion_key = df_filtrado["Ponderación"].values[0]  # Tomar el primer valor si hay duplicados
                            
                                # 🔹 Buscar la ponderación en la estructura de ponderaciones
                                ponderacion_puesto = ponderaciones.get(ponderacion_key, {})
                            
                                # 🔹 Obtener la ponderación específica para el tipo de pregunta
                                ponderacion = ponderacion_puesto.get(tipo_pregunta, 1)  # Si no hay, usa 1 por defecto
                                                        
                                # 🔹 Aplicar ponderación a la columna 'Valoración' directamente
                                return row["Valoración"] * ponderacion
    
                            # Convertir las respuestas en valores numéricos
                            df_nuevas_valoraciones["Valoración"] = df_nuevas_valoraciones.apply(
                            lambda row: opciones_respuestas[row["TipoConocimientos"]].get(row["Valoración"], 0), axis=1
                        )
                            df_nuevas_valoraciones["Valoración_Ponderada"] = df_nuevas_valoraciones.apply(ponderar_valoracion_puntu, axis=1)
    
                            # Sumar la valoración ponderada para obtener el total
                            valoracion_total = df_nuevas_valoraciones["Valoración_Ponderada"].sum()
                        
                            df_valoraciones_actualizadas = pd.concat([df_valoraciones_existentes, df_nuevas_valoraciones], ignore_index=True)
                            st.success("Valoraciones guardadas correctamente.")
                        
                            df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(
                                subset=['Nombre', 'idConocimiento'], keep='last')
                        
                            df_resultados = []
                            tprueb = t2[t2['Puesto'] == df_nuevas_valoraciones['Puesto'].iloc[0]]
                            tprueb = pd.DataFrame(tprueb)
                            tprueb = tprueb.iloc[:, :-1]  
                            cols_numericas = tprueb.columns[6:]  # Desde la columna Junior en adelante
                            tprueb[cols_numericas] = tprueb[cols_numericas].apply(pd.to_numeric, errors='coerce').fillna(0)
                            def ponderar_valoracion(row, *args, **kwargs):
                                puesto = row.get("Puesto", None)
                                tipo_pregunta = row.get("TipoPreguntas", None)  # Tomamos TipoPreguntas
                            
                                if puesto is None or tipo_pregunta is None:
                                    return 0  # Evita errores en caso de datos mal formateados
                            
                                # 🔹 Filtrar df_personas sin sobrescribirlo
                                df_filtrado = df_personas[df_personas['Puesto'] == puesto]
                            
                                if df_filtrado.empty:
                                    return 0  # Evita errores si el puesto no existe en df_personas
                            
                                # 🔹 Extraer la ponderación del puesto
                                ponderacion_key = df_filtrado["Ponderación"].values[0]  # Tomar el primer valor si hay duplicados
                            
                                # 🔹 Buscar la ponderación en la estructura de ponderaciones
                                ponderacion_puesto = ponderaciones.get(ponderacion_key, {})
                            
                                # 🔹 Obtener la ponderación específica para el tipo de pregunta
                                ponderacion = ponderacion_puesto.get(tipo_pregunta, 1)  # Si no hay, usa 1 por defecto
                                                        
                                # 🔹 Aplicar ponderación a las columnas de valores, excluyendo la última
                                return row.iloc[6:] * ponderacion
                            
                            # Aplicar la función a tprueb
                            tprueb_ponderado = tprueb.apply(ponderar_valoracion, axis=1)
    
                            suma_columnas = tprueb_ponderado.sum()
                            # Sumar todas las valoraciones numéricas
                            suma_columnas = pd.to_numeric(suma_columnas, errors='coerce')
                            # Asegurarnos de que suma_columnas sea un diccionario válido
                            suma_columnas = dict(suma_columnas)  # Convertir a diccionario si es necesario
                            
                            # Convertir valores a float para evitar errores de comparación
                            suma_columnas = {str(k).strip(): float(v) for k, v in suma_columnas.items()}
                            valoracion = pd.to_numeric(valoracion, errors='coerce')
                            df_filtrado = df_nuevas_valoraciones
                        
                            # Calcular la puntuación total
                            Nombre = df_filtrado.iloc[0]['Nombre']
                            Fecha = df_filtrado.iloc[0]['Fecha']
                            Evaluador = df_filtrado.iloc[0]['Evaluador']
                            ItinerarioNivel = df_filtrado.iloc[0]['ItinerarioNivel']
                            Departamento= df_filtrado.iloc[0]['Departamento']
                            seccion= df_filtrado.iloc[0]['Sección']
                            empresa= df_filtrado.iloc[0]['Empresa']
                            Ponderación= df_filtrado.iloc[0]['Ponderación']
                            Puesto = df_filtrado.iloc[0]['Puesto'].replace('\u00A0', '')
                            area= df_filtrado.iloc[0]['Área'].replace('\u00A0', '')
                            Nivel = None  # Nivel por defecto si no encuentra otro
                            ultimo_nivel = None  # Guarda el último nivel evaluado antes del correcto
                            penultimo_nivel_valido = None  # Guarda el penúltimo nivel válido (con valor > 0)
                            primer_nivel = next(iter(suma_columnas))  # Obtiene el primer nivel de la tabla
                            ultimo_nivel_valido= None
    
                            # Recorrer los niveles y comparar la valoración con los valores de referencia
                            for nivel, valor_referencia in suma_columnas.items():
                            
                                if valor_referencia > 0:  # Solo actualizar si el nivel tiene un valor mayor a 0
                                    if penultimo_nivel_valido is not None:
                                        ultimo_nivel_valido = penultimo_nivel_valido
                                    penultimo_nivel_valido = nivel  # Actualiza el penúltimo nivel válido
                            
                                if valor_referencia > 0 and valoracion_total < valor_referencia:
                                    # Asignar el penúltimo nivel válido antes de encontrar uno mayor
                                    if ultimo_nivel_valido:
                                        Nivel = ultimo_nivel_valido
                                        break  # Detener el ciclo una vez que se asigna un nivel
                            
                                ultimo_nivel = nivel  # Guarda el último nivel evaluado (aunque tenga valor 0)
                            
                            # Si no se asignó un nivel, asignar el penúltimo nivel válido
                            if Nivel is None:
                                if ultimo_nivel_valido:  # Si encontramos un nivel válido, asignamos el penúltimo nivel válido
                                    Nivel = ultimo_nivel_valido
                                else:  # Si no, asignamos el nivel con mayor valor
                                    max_nivel = max(suma_columnas, key=suma_columnas.get)  # Obtener el nivel con el valor más alto
                                    Nivel = max_nivel
                            
                            # Si la valoración es mayor que todos los niveles, asignar el nivel más alto
                            max_nivel = max(suma_columnas, key=suma_columnas.get)  # Obtener el nivel con el valor más alto
                            if valoracion_total >= suma_columnas[max_nivel]:  # Ahora incluye igualdad
                                Nivel = max_nivel                            
    
                            st.session_state.Nivel = Nivel 
                            bsresp = float(str(t33[(t33['Puesto'] == Puesto) & (t33['Nivel'] == Nivel)]['Rango Retributivo'].iloc[0]).replace(',', '.'))
                            BANDASALARIAL= bsresp
                            
                            df_filtrado_t4 = t4[(t4['Nombre'] == Nombre) & (t4['Puesto'] == Puesto)]
                            RetrVariable_Propuesta= 0
                            if not df_filtrado_t4.empty:
                                RETIBUCIÓNACTUAL = float(str(df_filtrado_t4['Salario Bruto Año'].iloc[0]).replace(',', '.'))
                                RetrVariable_Actual= float(str(df_filtrado_t4['Salario Variable Actual'].iloc[0]).replace(',', '.'))
                            else:
                                pass
                                RETIBUCIÓNACTUAL = 0.0
                                RetrVariable_Actual= 0.0
                            Diferencia_Retr = RETIBUCIÓNACTUAL-BANDASALARIAL
                            df_resultados.append({'Evaluador': Evaluador,
                                                  'Nombre': Nombre,
                                                  'Área': area,
                                                  'Puesto': Puesto,
                                                  'Departamento': Departamento,
                                                  'Sección': seccion,
                                                  'Empresa': empresa,
                                                  'Ponderación':Ponderación,
                                                  'Nivel': Nivel,
                                                  'ItinerarioNivel': ItinerarioNivel,
                                                  'Valoración_Obtenida': valoracion_total,
                                                  'Retribución_Actual': RETIBUCIÓNACTUAL,
                                                  'RetrVariable_Actual': RetrVariable_Actual,
                                                  'Retr_Propuesta': BANDASALARIAL,                                          
                                                  'RetrVariable_Propuesta': RetrVariable_Propuesta,
                                                  'Diferencia_Retr': Diferencia_Retr,
                                                  'Observación':observacion,
                                                  "Fecha": Fecha})
                            df_resultados=pd.DataFrame(df_resultados)
                            columnas_monetarias = ['Retribución_Actual',
                                    'RetrVariable_Actual', 'Retr_Propuesta', 'RetrVariable_Propuesta', 'Diferencia_Retr']
                                # Formatear las columnas numéricas
                            for col in columnas_monetarias:
                                df_resultados[col] = df_resultados[col].apply(lambda x: f"{x:,.2f} €" if pd.notnull(x) else "N/A")
                            df_resultados["Valoración_Obtenida"] = df_resultados["Valoración_Obtenida"].round().astype(int)
    
    
                            if 'df_valoraciones_actualizadas' in locals() and not df_valoraciones_actualizadas.empty:
                                df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
                                df_resultados = df_resultados.sort_values('Fecha').drop_duplicates(subset=['Nombre'], keep='last')
                                df_valoraciones_actualizadas['Departamento'].fillna('', inplace=True)
                                df_valoraciones_actualizadas['Sección'].fillna('', inplace=True)
                                df_resultados['Departamento'].fillna('', inplace=True)
                                df_resultados['Sección'].fillna('', inplace=True)
                                insertar_valoraciones_en_sql(df_valoraciones_actualizadas)
                                insertar_resultados_en_sql(df_resultados)
                    else:
                        st.warning(f"No hay ficha de desarrollo, por lo que no se puede proceder a la evaluación. Contactar con RRHH (imengual@sklum.com).")
                else:
                    st.warning("No se encontraron Nombres para este Evaluador.")
            if lat == 'Resultados':
                st.title("RESULTADOS DEL PROCESO DE EVALUACIÓN DEL DESEMPEÑO")                        
                df_valoraciones_actualizadas = ver_datos()
                df_resultados = ver_datos2()
                df_resultados = df_resultados[df_resultados["Área"] == area_usuario]
                df_resultados = df_resultados[df_resultados["Evaluador"] == usuario_autenticado]
                df_valoraciones_actualizadas = df_valoraciones_actualizadas[df_valoraciones_actualizadas["Evaluador"] == usuario_autenticado]
                df_valoraciones_actualizadas = df_valoraciones_actualizadas[df_valoraciones_actualizadas["Área"] == area_usuario]
                df_personas = df_personas[df_personas["Área"] == area_usuario]
                df_personas = df_personas[df_personas["SUPERVISOR"] == usuario_autenticado]


                st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)
                departamentos = df_personas["Departamento"].dropna().astype(str).unique()
                
                Departamento_seleccionado = st.selectbox(
                    'Selecciona el Departamento:',
                    ['Todos'] + sorted(departamentos),
                    label_visibility='collapsed'
                )
                
                # Filtrar ambos DataFrames por Departamento
                df_personas_filtrado = df_personas if Departamento_seleccionado == "Todos" else df_personas[df_personas["Departamento"] == Departamento_seleccionado]
                df_resultados_filtrado = df_resultados if Departamento_seleccionado == "Todos" else df_resultados[df_resultados["Departamento"] == Departamento_seleccionado]
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
                secciones = df_personas_filtrado["Sección"].dropna().astype(str).unique().tolist()
                
                seccion_seleccionada = st.selectbox(
                    'Selecciona la Sección:',
                    ['Todos'] + sorted(secciones),
                    label_visibility='collapsed'
                )
                
                # Filtrar por Sección
                df_personas_filtrado = df_personas_filtrado if seccion_seleccionada == "Todos" else df_personas_filtrado[df_personas_filtrado["Sección"] == seccion_seleccionada]
                df_resultados_filtrado = df_resultados_filtrado if seccion_seleccionada == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Sección"] == seccion_seleccionada]
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
                puestos = df_personas_filtrado["Puesto"].dropna().astype(str).unique().tolist()
                
                Puesto_seleccionado = st.selectbox(
                    'Selecciona el Puesto:',
                    ['Todos'] + sorted(puestos),
                    label_visibility='collapsed'
                )
                
                # Filtrar por Puesto
                df_personas_filtrado = df_personas_filtrado if Puesto_seleccionado == "Todos" else df_personas_filtrado[df_personas_filtrado["Puesto"] == Puesto_seleccionado]
                df_resultados_filtrado = df_resultados_filtrado if Puesto_seleccionado == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Puesto"] == Puesto_seleccionado]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)

                Empresa_seleccionada = st.selectbox(

                    'Selecciona la Empresa:',

                    ['Todos'] + sorted(df_filtrado['Empresa'].dropna().astype(str).unique().tolist()),

                    label_visibility='collapsed'

                )
 
                df_personas_filtrado = df_personas_filtrado if Empresa_seleccionada == "Todos" else df_personas_filtrado[df_personas_filtrado["Empresa"] == Empresa_seleccionada]
                df_resultados_filtrado = df_resultados_filtrado if Puesto_seleccionado == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Puesto"] == Puesto_seleccionado]
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Evaluador</h4>", unsafe_allow_html=True)
                evaluadores = df_personas_filtrado["SUPERVISOR"].dropna().astype(str).unique().tolist()
                
                evaluador_seleccionado = st.selectbox(
                    'Selecciona el Evaluador:',
                    ['Todos'] + sorted(evaluadores),
                    label_visibility='collapsed'
                )
                
                # Filtrar por Evaluador
                df_personas_filtrado = df_personas_filtrado if evaluador_seleccionado == "Todos" else df_personas_filtrado[df_personas_filtrado["SUPERVISOR"] == evaluador_seleccionado]
                df_resultados_filtrado = df_resultados_filtrado if evaluador_seleccionado == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Evaluador"] == evaluador_seleccionado]
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Nombre</h4>", unsafe_allow_html=True)
                nombres = df_personas_filtrado["Nombre"].dropna().astype(str).unique().tolist()
                
                Nombre_seleccionado = st.selectbox(
                    'Selecciona el Nombre:',
                    ['Todos'] + sorted(nombres),
                    label_visibility='collapsed'
                )
                
                # Filtrar por Nombre
                df_personas = df_personas_filtrado if Nombre_seleccionado == "Todos" else df_personas_filtrado[df_personas_filtrado["Nombre"] == Nombre_seleccionado]
                df_resultados = df_resultados_filtrado if Nombre_seleccionado == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Nombre"] == Nombre_seleccionado]

                df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
                df_resultados = df_resultados.sort_values('Valoración_Obtenida').drop_duplicates(subset=['Nombre'], keep='last')
                #df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
                #df_resultados = apply_filters(df_resultados, area_filter, Evaluador_filter,Puesto_filter, Nombre_filter)
  
                result_styled = df_resultados.style.applymap(highlight_cells, subset=['Diferencia_Retr'])
                st.markdown("### Rango de Niveles")
                
                # Crear columnas para distribuir los itinerarios en paralelo
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### Itinerario 3.1")
                    st.markdown("""
                    - **Junior**: 0 - 2 años  
                    - **Intermedio**: 2 - 4 años  
                    - **Senior**: > 4 años  
                    """)
                
                    st.markdown("#### Itinerario 4")
                    st.markdown("""
                    - **Junior**: 0 - 2 años  
                    - **Intermedio**: 2 - 5 años  
                    - **Avanzado**: 5 - 8 años  
                    - **Senior**: > 8 años  
                    """)
                
                with col2:
                    st.markdown("#### Itinerario 3.2")
                    st.markdown("""
                    - **Junior**: 0 - 3 años  
                    - **Intermedio**: 3 - 6 años  
                    - **Senior**: > 6 años  
                    """)
                
                    st.markdown("#### Itinerario 5")
                    st.markdown("""
                    - **Junior**: 0 - 2 años  
                    - **Intermedio**: 2 - 5 años  
                    - **Avanzado**: 5 - 8 años  
                    - **Experto**: 8 - 10 años  
                    - **Senior**: > 10 años  
                    """)

                result_styled = df_resultados.sort_values('Fecha').drop_duplicates(subset=['Nombre'], keep='last')
                # Mostrar resultados
                # Lista de columnas a excluir
                columnas_excluir = ["Observación", "Retribución_Actual", "RetrVariable_Actual", "Retr_Propuesta", "RetrVariable_Propuesta", "Diferencia_Retr"]  
                
                # Filtrar el DataFrame excluyendo esas columnas
                columnas_a_mostrar = [col for col in result_styled.columns if col not in columnas_excluir]
                st.table(result_styled[columnas_a_mostrar])
                
                excel_file2 = to_excel(df_resultados)
                
                st.download_button(
                    label="📥 Descargar Resultados",
                    data=excel_file2,
                    file_name="Resultados.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.markdown("### Observaciones")
                df_observaciones= df_resultados[["Evaluador", "Nombre", "Observación"]].dropna(subset=["Observación"])
                df_observaciones = df_observaciones[df_observaciones["Observación"].str.strip() != ""]
                excel_file6 = to_excel(df_observaciones)
                st.table(df_observaciones)
                st.download_button(
                label="📥Descargar Observaciones",
                data=excel_file6,
                file_name="Observaciones.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
                nivel_counts = df_resultados["Nivel"].value_counts().astype(int)
                # Crear el gráfico de barras en Streamlit
                st.markdown("### Distribución de Evaluaciones por Nivel")
                fig, ax = plt.subplots(figsize=(10, 6))
                ax.bar(nivel_counts.index, nivel_counts.values, color="skyblue")
                # Etiquetas y título
                ax.set_xlabel("Nivel")
                ax.set_ylabel("Cantidad de Evaluaciones")
                ax.set_title("Distribución de Evaluaciones por Nivel")
                ax.set_xticks(range(len(nivel_counts.index)))
                ax.set_xticklabels(nivel_counts.index, rotation=45)
                
                # Mostrar el gráfico en Streamlit
                st.pyplot(fig)
                df_seguimiento= df_resultados['Evaluador'].value_counts().reset_index()
                df_seguimiento.columns = ['Evaluador', 'Evaluaciones Realizadas']
                df_seguimiento['Nº Colaboradores'] = df_seguimiento['Evaluador'].apply(
                    lambda Evaluador: df_personas[df_personas["SUPERVISOR"] == Evaluador].shape[0]
                )
                df_seguimiento['% Consecución'] = df_seguimiento['Evaluaciones Realizadas'] / df_seguimiento['Nº Colaboradores'] * 100
                df_seguimiento['% Consecución'] = df_seguimiento['% Consecución'].map('{:.2f} %'.format)
                st.subheader("Seguimiento de Evaluación")
                st.table(df_seguimiento)
                excelfile2= to_excel(df_seguimiento)
                st.download_button(
                    label="📥 Descargar Seguimiento",
                    data=excelfile2,
                    file_name="Seguimiento.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.subheader("Personas Evaluadas")
                df_Evaluadas = df_resultados[['Nombre', 'Evaluador']]
                st.table(df_Evaluadas)
                excel_file4 = to_excel(df_Evaluadas)
                st.download_button(
                    label="📥 Personas Evaluadas",
                    data=excel_file4,
                    file_name="Personas_Evaluadas.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                 
    
                st.subheader("Personas por Evaluar")
                #Filtrar df_personas
                df_filtrado = df_personas if Nombre_seleccionado == "Todos" else df_personas[df_personas["Nombre"] == Nombre_seleccionado]
                df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
                df_filtrado = df_filtrado if evaluador_seleccionado == "Todos" else df_filtrado[df_filtrado["SUPERVISOR"] == evaluador_seleccionado]
                df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]
                df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]

                dfno_evaluados = df_personas[~df_personas['Nombre'].isin(df_Evaluadas['Nombre'])][['Nombre', 'SUPERVISOR']]
                dfno_evaluados = dfno_evaluados.rename(columns={'SUPERVISOR': 'Evaluador'})
                excel_file3 = to_excel(dfno_evaluados)
                st.table(dfno_evaluados)
                st.download_button(
                    label="📥 Personas No Evaluadas",
                    data=excel_file3,
                    file_name="Personas_No_Evaluadas.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            if lat == 'Resultados en detalle':
                Niveles = ['Junior 0 - 2 años', 'Intermedio 2 - 4 años', 'Senior > 4 años']

                st.subheader('RESULTADOS EN DETALLE')
            
                # Obtener los datos solo una vez
                df_resultados = ver_datos2()
                df_valoraciones_actualizadas = ver_datos()
                df_valoraciones_actualizadas = df_valoraciones_actualizadas[df_valoraciones_actualizadas["Área"] == area_usuario]
                df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
                df_resultados = df_resultados[df_resultados["Evaluador"] == usuario_autenticado]
                df_valoraciones_actualizadas = df_valoraciones_actualizadas[df_valoraciones_actualizadas["Evaluador"] == usuario_autenticado]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)
                departamentos = df_resultados["Departamento"].dropna().astype(str).unique()
                Departamento_seleccionado = st.selectbox(
                    'Selecciona el Departamento:',
                    ['Todos'] + sorted(departamentos),
                    label_visibility='collapsed'
                )
                df_filtrado = df_valoraciones_actualizadas if Departamento_seleccionado == "Todos" else df_valoraciones_actualizadas[df_valoraciones_actualizadas["Departamento"] == Departamento_seleccionado]                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
                # Asegurar que los valores son strings y eliminar NaN
                secciones = df_filtrado["Sección"].dropna().astype(str).unique().tolist()
                seccion_seleccionada = st.selectbox(
                    'Selecciona la Sección:',
                    ['Todos'] + sorted(secciones),
                    label_visibility='collapsed'
                )
                df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
                # Asegurar que los valores son strings y eliminar NaN
                puestos = df_filtrado["Puesto"].dropna().astype(str).unique().tolist()
                Puesto_seleccionado = st.selectbox(
                    'Selecciona el Puesto:',
                    ['Todos'] + sorted(puestos),
                    label_visibility='collapsed'
                )
                df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
 
                df_valoraciones_actualizadas = ver_datos()
                df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)
                Empresa_seleccionada = st.selectbox(
                    'Selecciona la Empresa:',
                    ['Todos'] + sorted(df_filtrado['Empresa'].dropna().astype(str).unique().tolist()),
                    label_visibility='collapsed'
                )
 
                df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]
                # Organizar las columnas
                df_valoraciones_actualizadas= df_filtrado            
                cols = [col for col in df_valoraciones_actualizadas.columns if col != 'Fecha'] + ['Fecha']
                df_valoraciones_actualizadas = df_valoraciones_actualizadas[cols]
            
                # Mostrar la tabla solo una vez
                st.table(df_valoraciones_actualizadas)
            
                # 📥 Descargar el archivo Excel
                excel_file = to_excel(df_valoraciones_actualizadas)
                st.download_button(
                    label="📥 Descargar Valoraciones",
                    data=excel_file,
                    file_name="Valoraciones.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    
        
        if direct=="Sí":
            
            lat = st.sidebar.selectbox('Menú',('Evaluar','Resultados Área', 'Resultados en detalle Área'))
            if lat=='Evaluar':
                st.title('PROCESO DE EVALUACIÓN DEL DESEMPEÑO')
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)
                departamentos = df_filtrado["Departamento"].dropna().astype(str).unique()
                Departamento_seleccionado = st.selectbox(
                    'Selecciona el Departamento:',
                    ['Todos'] + sorted(departamentos),
                    label_visibility='collapsed'
                )
                df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
                
                # Asegurar que los valores son strings y eliminar NaN
                secciones = df_filtrado["Sección"].dropna().astype(str).unique().tolist()
                
                seccion_seleccionada = st.selectbox(
                    'Selecciona la Sección:',
                    ['Todos'] + sorted(secciones),
                    label_visibility='collapsed'
                )
                
                df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
                
                # Asegurar que los valores son strings y eliminar NaN
                puestos = df_filtrado["Puesto"].dropna().astype(str).unique().tolist()
                
                Puesto_seleccionado = st.selectbox(
                    'Selecciona el Puesto:',
                    ['Todos'] + sorted(puestos),
                    label_visibility='collapsed'
                )
                
                df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]

                df_valoraciones_actualizadas = ver_datos()
                df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)

                Empresa_seleccionada = st.selectbox(

                    'Selecciona la Empresa:',

                    ['Todos'] + sorted(df_filtrado['Empresa'].dropna().astype(str).unique().tolist()),

                    label_visibility='collapsed'

                )
 
                df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]
 
                filtro_evaluacion = st.radio("Filtrar personas:",
            ["No evaluadas", "Evaluadas"])
                personas_evaluadas = df_valoraciones_actualizadas["Nombre"].str.strip().unique().tolist()
                df_filtrado["Evaluada"] = df_filtrado["Nombre"].str.strip().isin(personas_evaluadas)
                if filtro_evaluacion == "No evaluadas":
                    df_filtrado = df_filtrado[~df_filtrado["Evaluada"]]
                else:
                    df_filtrado = df_filtrado[df_filtrado["Evaluada"]]
                
                
                    # Determinar la persona seleccionada
                Nombre_seleccionado = st.selectbox("Selecciona una persona a valorar:", df_filtrado["Nombre"].unique())
                if not df_filtrado.empty:
                    
                    df_valoraciones_actualizadas = ver_datos()  # Cargar las valoraciones guardadas
                    
                    evaluada = Nombre_seleccionado.strip() in df_valoraciones_actualizadas["Nombre"].str.strip().values
                    if evaluada:
                        st.markdown('<p style="color:red;">Persona ya evaluada</p>', unsafe_allow_html=True)
            
                    persona = df_filtrado[df_filtrado["Nombre"] == Nombre_seleccionado].iloc[0]
                    area_persona = persona["Área"]
                    Puesto_persona = persona["Puesto"]
                    Departamento_persona = persona["Departamento"]
                    seccion_persona = persona["Sección"]
                    empresa_persona= persona["Empresa"]
                    ItinerarioNivel = persona["ItinerarioNivel"]
                    Ponderación= persona["Ponderación"]
            
                    st.write(f"Área: **{area_persona}** | Puesto: **{Puesto_persona}**")
            
                    Conocimientos = df_Puesto_pregs[(df_Puesto_pregs["Área"] == area_persona) & 
                                                    (df_Puesto_pregs["Puesto"] == Puesto_persona)]
                    
                    if not Conocimientos.empty:
                        st.markdown("**Instrucciones del cuestionario:**")
                        valoraciones = []
                        Fecha_actual = datetime.now()
            
                        st.markdown("""
                        En el presente cuestionario aparecen los criterios definidos (Responsabilidades, Conocimientos funcionales y Competencias) y validados en la ficha de desarrollo.<br><br>
                        El objetivo del cuestionario consiste en realizar la valoración de dichos criterios. Esta información nos será útil, solo si se responde de una manera sincera y objetiva.<br><br>
                        Gracias por tu colaboración.
                        """, unsafe_allow_html=True)
            
                        tipo_actual = None
                        
                        for i in range(len(Conocimientos)):
                            row = Conocimientos.iloc[i]
                            Conocimiento = row["Conocimiento"]
                            tipo_Conocimiento = row["TipoPreguntas"]
                            opciones = opciones_respuestas.get(tipo_Conocimiento, ["No disponible"])
                            idConocimiento = row["ID Conocimiento"]
            
                            # Asegurar que "Nombre" y "idConocimiento" sean del mismo tipo en df_valoraciones_actualizadas
                            df_valoraciones_actualizadas["Nombre"] = df_valoraciones_actualizadas["Nombre"].str.strip()
                            df_valoraciones_actualizadas["idConocimiento"] = df_valoraciones_actualizadas["idConocimiento"].astype(str)
                            idConocimiento = str(idConocimiento)  # Convertimos la variable al mismo tipo
                            
                            # Buscar la respuesta previa si la persona ya fue evaluada
                            if evaluada:
                                valoracion_previa = df_valoraciones_actualizadas.query(
                                    "Nombre == @Nombre_seleccionado and idConocimiento == @idConocimiento"
                                )["Valoración"]
                                
                                # Si hay una respuesta previa, tomarla; si no, None
                                if not valoracion_previa.empty:
                                    valoracion_seleccionada = valoracion_previa.iloc[0]  # Usa iloc en lugar de values[0]
                                else:
                                    valoracion_seleccionada = None
                            else:
                                valoracion_seleccionada = None
        
                            # Agregar título del tipo de Conocimiento si cambia
                            if tipo_Conocimiento != tipo_actual:
                                st.subheader(f"{tipo_Conocimiento}")
                                tipo_actual = tipo_Conocimiento
            
                            st.markdown(f"""
                                <div style="font-weight: bold; font-size: 18px; margin-bottom: -15px;">
                                    {Conocimiento}
                                </div>""", unsafe_allow_html=True)
                            
                            # Preseleccionar la respuesta previa en el radio button
                            valoracion = st.radio(
                                "", opciones, key=f"Conocimiento_{idConocimiento}", index=opciones.index(valoracion_seleccionada) if valoracion_seleccionada in opciones else 0
                            )
                            
            
                            valoraciones.append({
                                "Evaluador": usuario_autenticado,
                                "Nombre": Nombre_seleccionado,
                                "Área": area_persona,
                                "Puesto": Puesto_persona,
                                "Departamento": Departamento_persona,
                                "Sección": seccion_persona,
                                "Empresa": empresa_persona,
                                "Ponderación": Ponderación,
                                "ItinerarioNivel": ItinerarioNivel,
                                "idConocimiento": idConocimiento,
                                "TipoConocimientos": tipo_Conocimiento,
                                "Conocimiento": Conocimiento,
                                "Valoración": valoracion,
                                "Fecha": (Fecha_actual + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
                            })
        
        
                        observacion = st.text_area(
                            "Añadir observación (opcional):", 
                            key=f"Observacion_{idConocimiento}",
                            help="Puedes escribir comentarios adicionales sobre la evaluación.")    
                        if st.button("Guardar valoraciones"):
                            
                            df_nuevas_valoraciones = pd.DataFrame(valoraciones)
                            def ponderar_valoracion_puntu(row, *args, **kwargs):
                                puesto = row.get("Puesto", None)
                                tipo_pregunta = row.get("TipoConocimientos", None)  # Tomamos TipoPreguntas
                            
                                if puesto is None or tipo_pregunta is None:
                                    return 0  # Evita errores en caso de datos mal formateados
                            
                                # 🔹 Filtrar df_personas sin sobrescribirlo
                                df_filtrado = df_personas[df_personas['Puesto'] == puesto]
                            
                                if df_filtrado.empty:
                                    return 0  # Evita errores si el puesto no existe en df_personas
                            
                                # 🔹 Extraer la ponderación del puesto
                                ponderacion_key = df_filtrado["Ponderación"].values[0]  # Tomar el primer valor si hay duplicados
                            
                                # 🔹 Buscar la ponderación en la estructura de ponderaciones
                                ponderacion_puesto = ponderaciones.get(ponderacion_key, {})
                            
                                # 🔹 Obtener la ponderación específica para el tipo de pregunta
                                ponderacion = ponderacion_puesto.get(tipo_pregunta, 1)  # Si no hay, usa 1 por defecto
                                                        
                                # 🔹 Aplicar ponderación a la columna 'Valoración' directamente
                                return row["Valoración"] * ponderacion

                            # Convertir las respuestas en valores numéricos
                            df_nuevas_valoraciones["Valoración"] = df_nuevas_valoraciones.apply(
                            lambda row: opciones_respuestas[row["TipoConocimientos"]].get(row["Valoración"], 0), axis=1
                        )
                            df_nuevas_valoraciones["Valoración_Ponderada"] = df_nuevas_valoraciones.apply(ponderar_valoracion_puntu, axis=1)

                            # Sumar la valoración ponderada para obtener el total
                            valoracion_total = df_nuevas_valoraciones["Valoración_Ponderada"].sum()
                        
                            df_valoraciones_actualizadas = pd.concat([df_valoraciones_existentes, df_nuevas_valoraciones], ignore_index=True)
                            st.success("Valoraciones guardadas correctamente.")
                        
                            df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(
                                subset=['Nombre', 'idConocimiento'], keep='last')
                        
                            df_resultados = []
                            tprueb = t2[t2['Puesto'] == df_nuevas_valoraciones['Puesto'].iloc[0]]
                            tprueb = pd.DataFrame(tprueb)
                            tprueb = tprueb.iloc[:, :-1]  
                            cols_numericas = tprueb.columns[6:]  # Desde la columna Junior en adelante
                            tprueb[cols_numericas] = tprueb[cols_numericas].apply(pd.to_numeric, errors='coerce').fillna(0)
                            def ponderar_valoracion(row, *args, **kwargs):
                                puesto = row.get("Puesto", None)
                                tipo_pregunta = row.get("TipoPreguntas", None)  # Tomamos TipoPreguntas
                            
                                if puesto is None or tipo_pregunta is None:
                                    return 0  # Evita errores en caso de datos mal formateados
                            
                                # 🔹 Filtrar df_personas sin sobrescribirlo
                                df_filtrado = df_personas[df_personas['Puesto'] == puesto]
                            
                                if df_filtrado.empty:
                                    return 0  # Evita errores si el puesto no existe en df_personas
                            
                                # 🔹 Extraer la ponderación del puesto
                                ponderacion_key = df_filtrado["Ponderación"].values[0]  # Tomar el primer valor si hay duplicados
                            
                                # 🔹 Buscar la ponderación en la estructura de ponderaciones
                                ponderacion_puesto = ponderaciones.get(ponderacion_key, {})
                            
                                # 🔹 Obtener la ponderación específica para el tipo de pregunta
                                ponderacion = ponderacion_puesto.get(tipo_pregunta, 1)  # Si no hay, usa 1 por defecto
                                                        
                                # 🔹 Aplicar ponderación a las columnas de valores, excluyendo la última
                                return row.iloc[6:] * ponderacion
                            
                            # Aplicar la función a tprueb
                            tprueb_ponderado = tprueb.apply(ponderar_valoracion, axis=1)

                            suma_columnas = tprueb_ponderado.sum()
                            # Sumar todas las valoraciones numéricas
                            suma_columnas = pd.to_numeric(suma_columnas, errors='coerce')
                            # Asegurarnos de que suma_columnas sea un diccionario válido
                            suma_columnas = dict(suma_columnas)  # Convertir a diccionario si es necesario
                            
                            # Convertir valores a float para evitar errores de comparación
                            suma_columnas = {str(k).strip(): float(v) for k, v in suma_columnas.items()}
                            valoracion = pd.to_numeric(valoracion, errors='coerce')
                            df_filtrado = df_nuevas_valoraciones
                        
                            # Calcular la puntuación total
                            Nombre = df_filtrado.iloc[0]['Nombre']
                            Fecha = df_filtrado.iloc[0]['Fecha']
                            Evaluador = df_filtrado.iloc[0]['Evaluador']
                            empresa= df_filtrado.iloc[0]['Empresa']
                            ItinerarioNivel = df_filtrado.iloc[0]['ItinerarioNivel']
                            Departamento= df_filtrado.iloc[0]['Departamento']
                            seccion= df_filtrado.iloc[0]['Sección']
                            Ponderación= df_filtrado.iloc[0]['Ponderación']
                            Puesto = df_filtrado.iloc[0]['Puesto'].replace('\u00A0', '')
                            area= df_filtrado.iloc[0]['Área'].replace('\u00A0', '')
                            Nivel = None  # Nivel por defecto si no encuentra otro
                            ultimo_nivel = None  # Guarda el último nivel evaluado antes del correcto
                            penultimo_nivel_valido = None  # Guarda el penúltimo nivel válido (con valor > 0)
                            primer_nivel = next(iter(suma_columnas))  # Obtiene el primer nivel de la tabla
                            ultimo_nivel_valido= None

                            # Recorrer los niveles y comparar la valoración con los valores de referencia
                            for nivel, valor_referencia in suma_columnas.items():
                            
                                if valor_referencia > 0:  # Solo actualizar si el nivel tiene un valor mayor a 0
                                    if penultimo_nivel_valido is not None:
                                        ultimo_nivel_valido = penultimo_nivel_valido
                                    penultimo_nivel_valido = nivel  # Actualiza el penúltimo nivel válido
                            
                                if valor_referencia > 0 and valoracion_total < valor_referencia:
                                    # Asignar el penúltimo nivel válido antes de encontrar uno mayor
                                    if ultimo_nivel_valido:
                                        Nivel = ultimo_nivel_valido
                                        break  # Detener el ciclo una vez que se asigna un nivel
                            
                                ultimo_nivel = nivel  # Guarda el último nivel evaluado (aunque tenga valor 0)
                            
                            # Si no se asignó un nivel, asignar el penúltimo nivel válido
                            if Nivel is None:
                                if ultimo_nivel_valido:  # Si encontramos un nivel válido, asignamos el penúltimo nivel válido
                                    Nivel = ultimo_nivel_valido
                                else:  # Si no, asignamos el nivel con mayor valor
                                    max_nivel = max(suma_columnas, key=suma_columnas.get)  # Obtener el nivel con el valor más alto
                                    Nivel = max_nivel
                            
                            # Si la valoración es mayor que todos los niveles, asignar el nivel más alto
                            max_nivel = max(suma_columnas, key=suma_columnas.get)  # Obtener el nivel con el valor más alto
                            if valoracion_total >= suma_columnas[max_nivel]:  # Ahora incluye igualdad
                                Nivel = max_nivel                            

                            st.session_state.Nivel = Nivel 
                            bsresp = float(str(t33[(t33['Puesto'] == Puesto) & (t33['Nivel'] == Nivel)]['Rango Retributivo'].iloc[0]).replace(',', '.'))
                            BANDASALARIAL= bsresp
                            
                            df_filtrado_t4 = t4[(t4['Nombre'] == Nombre) & (t4['Puesto'] == Puesto)]
                            RetrVariable_Propuesta= 0
                            if not df_filtrado_t4.empty:
                                RETIBUCIÓNACTUAL = float(str(df_filtrado_t4['Salario Bruto Año'].iloc[0]).replace(',', '.'))
                                RetrVariable_Actual= float(str(df_filtrado_t4['Salario Variable Actual'].iloc[0]).replace(',', '.'))
                            else:
                                pass
                                RETIBUCIÓNACTUAL = 0.0
                                RetrVariable_Actual= 0.0
                            Diferencia_Retr = RETIBUCIÓNACTUAL-BANDASALARIAL
                            df_resultados.append({'Evaluador': Evaluador,
                                                  'Nombre': Nombre,
                                                  'Área': area,
                                                  'Puesto': Puesto,
                                                  'Departamento': Departamento,
                                                  'Sección': seccion,
                                                  'Empresa':empresa,
                                                  'Ponderación':Ponderación,
                                                  'Nivel': Nivel,
                                                  'ItinerarioNivel': ItinerarioNivel,
                                                  'Valoración_Obtenida': valoracion_total,
                                                  'Retribución_Actual': RETIBUCIÓNACTUAL,
                                                  'RetrVariable_Actual': RetrVariable_Actual,
                                                  'Retr_Propuesta': BANDASALARIAL,                                          
                                                  'RetrVariable_Propuesta': RetrVariable_Propuesta,
                                                  'Diferencia_Retr': Diferencia_Retr,
                                                  'Observación':observacion,
                                                  "Fecha": Fecha})
                            df_resultados=pd.DataFrame(df_resultados)
                            columnas_monetarias = ['Retribución_Actual',
                                    'RetrVariable_Actual', 'Retr_Propuesta', 'RetrVariable_Propuesta', 'Diferencia_Retr']
                                # Formatear las columnas numéricas
                            for col in columnas_monetarias:
                                df_resultados[col] = df_resultados[col].apply(lambda x: f"{x:,.2f} €" if pd.notnull(x) else "N/A")
                            df_resultados["Valoración_Obtenida"] = df_resultados["Valoración_Obtenida"].round().astype(int)
        
        
                            if 'df_valoraciones_actualizadas' in locals() and not df_valoraciones_actualizadas.empty:
                                df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
                                df_resultados = df_resultados.sort_values('Fecha').drop_duplicates(subset=['Nombre'], keep='last')
                                df_valoraciones_actualizadas['Departamento'].fillna('', inplace=True)
                                df_valoraciones_actualizadas['Sección'].fillna('', inplace=True)
                                df_resultados['Departamento'].fillna('', inplace=True)
                                df_resultados['Sección'].fillna('', inplace=True)

                                insertar_valoraciones_en_sql(df_valoraciones_actualizadas)
                                insertar_resultados_en_sql(df_resultados)
                    else:
                        st.warning(f"No hay ficha de desarrollo, por lo que no se puede proceder a la evaluación. Contactar con RRHH (imengual@sklum.com).")
                else:
                    st.warning("No se encontraron Nombres para este Evaluador.")
            if lat == 'Resultados Área':
                st.title("RESULTADOS DEL PROCESO DE EVALUACIÓN DEL DESEMPEÑO")                        
                df_valoraciones_actualizadas = ver_datos()
                df_resultados = ver_datos2()
                df_resultados = df_resultados[df_resultados["Área"] == area_usuario]
                df_valoraciones_actualizadas = df_valoraciones_actualizadas[df_valoraciones_actualizadas["Área"] == area_usuario]
                df_personas = df_personas[df_personas["Área"] == area_usuario]
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)
                departamentos = df_personas["Departamento"].dropna().astype(str).unique()
                
                Departamento_seleccionado = st.selectbox(
                    'Selecciona el Departamento:',
                    ['Todos'] + sorted(departamentos),
                    label_visibility='collapsed'
                )
                
                # Filtrar ambos DataFrames por Departamento
                df_personas_filtrado = df_personas if Departamento_seleccionado == "Todos" else df_personas[df_personas["Departamento"] == Departamento_seleccionado]
                df_resultados_filtrado = df_resultados if Departamento_seleccionado == "Todos" else df_resultados[df_resultados["Departamento"] == Departamento_seleccionado]
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
                secciones = df_personas_filtrado["Sección"].dropna().astype(str).unique().tolist()
                
                seccion_seleccionada = st.selectbox(
                    'Selecciona la Sección:',
                    ['Todos'] + sorted(secciones),
                    label_visibility='collapsed'
                )
                
                # Filtrar por Sección
                df_personas_filtrado = df_personas_filtrado if seccion_seleccionada == "Todos" else df_personas_filtrado[df_personas_filtrado["Sección"] == seccion_seleccionada]
                df_resultados_filtrado = df_resultados_filtrado if seccion_seleccionada == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Sección"] == seccion_seleccionada]
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
                puestos = df_personas_filtrado["Puesto"].dropna().astype(str).unique().tolist()
                
                Puesto_seleccionado = st.selectbox(
                    'Selecciona el Puesto:',
                    ['Todos'] + sorted(puestos),
                    label_visibility='collapsed'
                )
                
                # Filtrar por Puesto
                df_personas_filtrado = df_personas_filtrado if Puesto_seleccionado == "Todos" else df_personas_filtrado[df_personas_filtrado["Puesto"] == Puesto_seleccionado]
                df_resultados_filtrado = df_resultados_filtrado if Puesto_seleccionado == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Puesto"] == Puesto_seleccionado]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)
                Empresa_seleccionada = st.selectbox(
                    'Selecciona la Empresa:',
                    ['Todos'] + sorted(df_personas_filtrado['Empresa'].unique().tolist()),
                    label_visibility='collapsed'
                )
                df_personas_filtrado = df_personas_filtrado if Empresa_seleccionada == "Todos" else df_personas_filtrado[df_personas_filtrado["Empresa"] == Empresa_seleccionada]
                df_resultados_filtrado = df_resultados_filtrado if Empresa_seleccionada == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Empresa"] == Empresa_seleccionada]
               
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Evaluador</h4>", unsafe_allow_html=True)
                evaluadores = df_personas_filtrado["SUPERVISOR"].dropna().astype(str).unique().tolist()
                
                evaluador_seleccionado = st.selectbox(
                    'Selecciona el Evaluador:',
                    ['Todos'] + sorted(evaluadores),
                    label_visibility='collapsed'
                )
                
                # Filtrar por Evaluador
                df_personas_filtrado = df_personas_filtrado if evaluador_seleccionado == "Todos" else df_personas_filtrado[df_personas_filtrado["SUPERVISOR"] == evaluador_seleccionado]
                df_resultados_filtrado = df_resultados_filtrado if evaluador_seleccionado == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Evaluador"] == evaluador_seleccionado]
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Nombre</h4>", unsafe_allow_html=True)
                nombres = df_personas_filtrado["Nombre"].dropna().astype(str).unique().tolist()
                
                Nombre_seleccionado = st.selectbox(
                    'Selecciona el Nombre:',
                    ['Todos'] + sorted(nombres),
                    label_visibility='collapsed'
                )
                
                # Filtrar por Nombre
                df_personas = df_personas_filtrado if Nombre_seleccionado == "Todos" else df_personas_filtrado[df_personas_filtrado["Nombre"] == Nombre_seleccionado]
                df_resultados = df_resultados_filtrado if Nombre_seleccionado == "Todos" else df_resultados_filtrado[df_resultados_filtrado["Nombre"] == Nombre_seleccionado]

                df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
                df_resultados = df_resultados.sort_values('Valoración_Obtenida').drop_duplicates(subset=['Nombre'], keep='last')
                #df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
                #df_resultados = apply_filters(df_resultados, area_filter, Evaluador_filter,Puesto_filter, Nombre_filter)
  
                result_styled = df_resultados.style.applymap(highlight_cells, subset=['Diferencia_Retr'])
                st.markdown("### Rango de Niveles")
                
                # Crear columnas para distribuir los itinerarios en paralelo
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### Itinerario 3.1")
                    st.markdown("""
                    - **Junior**: 0 - 2 años  
                    - **Intermedio**: 2 - 4 años  
                    - **Senior**: > 4 años  
                    """)
                
                    st.markdown("#### Itinerario 4")
                    st.markdown("""
                    - **Junior**: 0 - 2 años  
                    - **Intermedio**: 2 - 5 años  
                    - **Avanzado**: 5 - 8 años  
                    - **Senior**: > 8 años  
                    """)
                
                with col2:
                    st.markdown("#### Itinerario 3.2")
                    st.markdown("""
                    - **Junior**: 0 - 3 años  
                    - **Intermedio**: 3 - 6 años  
                    - **Senior**: > 6 años  
                    """)
                
                    st.markdown("#### Itinerario 5")
                    st.markdown("""
                    - **Junior**: 0 - 2 años  
                    - **Intermedio**: 2 - 5 años  
                    - **Avanzado**: 5 - 8 años  
                    - **Experto**: 8 - 10 años  
                    - **Senior**: > 10 años  
                    """)

                result_styled = df_resultados.sort_values('Fecha').drop_duplicates(subset=['Nombre'], keep='last')
                # Mostrar resultados
                # Lista de columnas a excluir
                columnas_excluir = ["Observación", "Retribución_Actual", "RetrVariable_Actual", "Retr_Propuesta", "RetrVariable_Propuesta", "Diferencia_Retr"]  
                
                # Filtrar el DataFrame excluyendo esas columnas
                columnas_a_mostrar = [col for col in result_styled.columns if col not in columnas_excluir]
                st.table(result_styled[columnas_a_mostrar])
                
                excel_file2 = to_excel(df_resultados)
                
                st.download_button(
                    label="📥 Descargar Resultados",
                    data=excel_file2,
                    file_name="Resultados.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.markdown("### Observaciones")
                df_observaciones= df_resultados[["Evaluador", "Nombre", "Observación"]].dropna(subset=["Observación"])
                df_observaciones = df_observaciones[df_observaciones["Observación"].str.strip() != ""]
                excel_file6 = to_excel(df_observaciones)
                st.table(df_observaciones)
                st.download_button(
                label="📥Descargar Observaciones",
                data=excel_file6,
                file_name="Observaciones.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
                nivel_counts = df_resultados["Nivel"].value_counts().astype(int)
                # Crear el gráfico de barras en Streamlit
                st.markdown("### Distribución de Evaluaciones por Nivel")
                fig, ax = plt.subplots(figsize=(10, 6))
                ax.bar(nivel_counts.index, nivel_counts.values, color="skyblue")
                # Etiquetas y título
                ax.set_xlabel("Nivel")
                ax.set_ylabel("Cantidad de Evaluaciones")
                ax.set_title("Distribución de Evaluaciones por Nivel")
                ax.set_xticks(range(len(nivel_counts.index)))
                ax.set_xticklabels(nivel_counts.index, rotation=45)
                
                # Mostrar el gráfico en Streamlit
                st.pyplot(fig)
                df_seguimiento= df_resultados['Evaluador'].value_counts().reset_index()
                df_seguimiento.columns = ['Evaluador', 'Evaluaciones Realizadas']
                df_seguimiento['Nº Colaboradores'] = df_seguimiento['Evaluador'].apply(
                    lambda Evaluador: df_personas[df_personas["SUPERVISOR"] == Evaluador].shape[0]
                )
                df_seguimiento['% Consecución'] = df_seguimiento['Evaluaciones Realizadas'] / df_seguimiento['Nº Colaboradores'] * 100
                df_seguimiento['% Consecución'] = df_seguimiento['% Consecución'].map('{:.2f} %'.format)
                st.subheader("Seguimiento de Evaluación")
                st.table(df_seguimiento)
                excelfile2= to_excel(df_seguimiento)
                st.download_button(
                    label="📥 Descargar Seguimiento",
                    data=excelfile2,
                    file_name="Seguimiento.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                st.subheader("Personas Evaluadas")
                df_Evaluadas = df_resultados[['Nombre', 'Evaluador']]
                st.table(df_Evaluadas)
                excel_file4 = to_excel(df_Evaluadas)
                st.download_button(
                    label="📥 Personas Evaluadas",
                    data=excel_file4,
                    file_name="Personas_Evaluadas.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                 
    
                st.subheader("Personas por Evaluar")
                #Filtrar df_personas
                df_filtrado = df_personas if Nombre_seleccionado == "Todos" else df_personas[df_personas["Nombre"] == Nombre_seleccionado]
                df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
                df_filtrado = df_filtrado if evaluador_seleccionado == "Todos" else df_filtrado[df_filtrado["SUPERVISOR"] == evaluador_seleccionado]
                df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]
                df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
                df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]

                dfno_evaluados = df_personas[~df_personas['Nombre'].isin(df_Evaluadas['Nombre'])][['Nombre', 'SUPERVISOR']]
                dfno_evaluados = dfno_evaluados.rename(columns={'SUPERVISOR': 'Evaluador'})
                excel_file3 = to_excel(dfno_evaluados)
                st.table(dfno_evaluados)
                st.download_button(
                    label="📥 Personas No Evaluadas",
                    data=excel_file3,
                    file_name="Personas_No_Evaluadas.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            if lat == 'Resultados en detalle Área':
                Niveles = ['Junior 0 - 2 años', 'Intermedio 2 - 4 años', 'Senior > 4 años']
            
                st.subheader('RESULTADOS EN DETALLE')
            
                # Obtener los datos solo una vez
                df_resultados = ver_datos2()
                df_valoraciones_actualizadas = ver_datos()
                df_valoraciones_actualizadas = df_valoraciones_actualizadas[df_valoraciones_actualizadas["Área"] == area_usuario]
                df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)
                departamentos = df_resultados["Departamento"].dropna().astype(str).unique()
                Departamento_seleccionado = st.selectbox(
                    'Selecciona el Departamento:',
                    ['Todos'] + sorted(departamentos),
                    label_visibility='collapsed'
                )
                df_filtrado = df_valoraciones_actualizadas if Departamento_seleccionado == "Todos" else df_valoraciones_actualizadas[df_valoraciones_actualizadas["Departamento"] == Departamento_seleccionado]                
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
                # Asegurar que los valores son strings y eliminar NaN
                secciones = df_filtrado["Sección"].dropna().astype(str).unique().tolist()
                seccion_seleccionada = st.selectbox(
                    'Selecciona la Sección:',
                    ['Todos'] + sorted(secciones),
                    label_visibility='collapsed'
                )
                df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
                # Asegurar que los valores son strings y eliminar NaN
                puestos = df_filtrado["Puesto"].dropna().astype(str).unique().tolist()
                Puesto_seleccionado = st.selectbox(
                    'Selecciona el Puesto:',
                    ['Todos'] + sorted(puestos),
                    label_visibility='collapsed'
                )
                df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
                st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)
                Empresa_seleccionada = st.selectbox(
                    'Selecciona la Empresa:',
                    ['Todos'] + sorted(df_filtrado['Empresa'].dropna().astype(str).unique().tolist()),
                    label_visibility='collapsed'
                )
 
                df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]
                df_valoraciones_actualizadas = ver_datos()
                df_valoraciones_actualizadas= df_filtrado            
    
                # Organizar las columnas
                cols = [col for col in df_valoraciones_actualizadas.columns if col != 'Fecha'] + ['Fecha']
                df_valoraciones_actualizadas = df_valoraciones_actualizadas[cols]
            
                # Mostrar la tabla solo una vez
                st.table(df_valoraciones_actualizadas)
            
                # 📥 Descargar el archivo Excel
                excel_file = to_excel(df_valoraciones_actualizadas)
                st.download_button(
                    label="📥 Descargar Valoraciones",
                    data=excel_file,
                    file_name="Valoraciones.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )



    
    elif usuario_autenticado == "admin2":
        lat = st.sidebar.selectbox('Menú',('Resultados','Resultados en detalle'))
        if lat == 'Resultados':
            st.title("RESULTADOS DEL PROCESO DE EVALUACIÓN DEL DESEMPEÑO")                        
            df_valoraciones_actualizadas = ver_datos()
            df_resultados = ver_datos2()
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Área</h4>", unsafe_allow_html=True)
            area_seleccionada = st.selectbox(
                'Selecciona el Área:',
                ['Todos'] + sorted(df_resultados['Área'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_resultados if area_seleccionada == "Todos" else df_resultados[df_resultados["Área"] == area_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)

            Departamento_seleccionado = st.selectbox(
                'Selecciona el Departamento:',
                ['Todos'] + sorted(df_filtrado['Departamento'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
            seccion_seleccionada = st.selectbox(
                'Selecciona la Sección:',
                ['Todos'] + sorted(df_filtrado['Sección'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
            Puesto_seleccionado = st.selectbox(
                'Selecciona el Puesto:',
                ['Todos'] + sorted(df_filtrado['Puesto'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)
            Empresa_seleccionada = st.selectbox(
                    'Selecciona la Empresa:',
                    ['Todos'] + sorted(df_filtrado['Empresa'].dropna().astype(str).unique().tolist()),
                    label_visibility='collapsed'
                )
 
            df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Evaluador</h4>", unsafe_allow_html=True)
            evaluador_seleccionado = st.selectbox(
                'Selecciona el Evaluador:',
                ['Todos'] + sorted(df_filtrado['Evaluador'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if evaluador_seleccionado == "Todos" else df_filtrado[df_filtrado["Evaluador"] == evaluador_seleccionado]

            st.markdown("<h4 style='font-size: 16px;'>Filtro por Nombre</h4>", unsafe_allow_html=True)
            Nombre_seleccionado = st.selectbox(
                'Selecciona el Nombre:',
                ['Todos'] + sorted(df_resultados['Nombre'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Nombre_seleccionado == "Todos" else df_filtrado[df_filtrado["Nombre"] == Nombre_seleccionado]
            df_resultados= df_filtrado
                    
            df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
            df_resultados = df_resultados.sort_values('Valoración_Obtenida').drop_duplicates(subset=['Nombre'], keep='last')
            #df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
            #df_resultados = apply_filters(df_resultados, area_filter, Evaluador_filter,Puesto_filter, Nombre_filter)


            result_styled = df_resultados.style.applymap(highlight_cells, subset=['Diferencia_Retr'])
            st.markdown("### Rango de Niveles")

            # Crear columnas para distribuir los itinerarios en paralelo
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Itinerario 3.1")
                st.markdown("""
                - **Junior**: 0 - 2 años  
                - **Intermedio**: 2 - 4 años  
                - **Senior**: > 4 años  
                """)
            
                st.markdown("#### Itinerario 4")
                st.markdown("""
                - **Junior**: 0 - 2 años  
                - **Intermedio**: 2 - 5 años  
                - **Avanzado**: 5 - 8 años  
                - **Senior**: > 8 años  
                """)
            
            with col2:
                st.markdown("#### Itinerario 3.2")
                st.markdown("""
                - **Junior**: 0 - 3 años  
                - **Intermedio**: 3 - 6 años  
                - **Senior**: > 6 años  
                """)
            
                st.markdown("#### Itinerario 5")
                st.markdown("""
                - **Junior**: 0 - 2 años  
                - **Intermedio**: 2 - 5 años  
                - **Avanzado**: 5 - 8 años  
                - **Experto**: 8 - 10 años  
                - **Senior**: > 10 años  
                """)

            result_styled = df_resultados.sort_values('Fecha').drop_duplicates(subset=['Nombre'], keep='last')
            # Mostrar resultados
            columnas_excluir = ["Observación", "Retribución_Actual", "RetrVariable_Actual", "Retr_Propuesta", "RetrVariable_Propuesta", "Diferencia_Retr"]  
            
            # Filtrar el DataFrame excluyendo esas columnas
            columnas_a_mostrar = [col for col in result_styled.columns if col not in columnas_excluir]
            st.table(result_styled[columnas_a_mostrar])
            
            excel_file2 = to_excel(df_resultados)
            
            st.download_button(
                label="📥 Descargar Resultados",
                data=excel_file2,
                file_name="Resultados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.markdown("### Observaciones")
            df_observaciones= df_resultados[["Evaluador", "Nombre", "Observación"]].dropna(subset=["Observación"])
            df_observaciones = df_observaciones[df_observaciones["Observación"].str.strip() != ""]
            excel_file6 = to_excel(df_observaciones)
            st.table(df_observaciones)
            st.download_button(
            label="📥 Descargar Observaciones",
            data=excel_file6,
            file_name="Observaciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
            nivel_counts = df_resultados["Nivel"].value_counts().astype(int)
            # Crear el gráfico de barras en Streamlit
            st.markdown("### Distribución de Evaluaciones por Nivel")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(nivel_counts.index, nivel_counts.values, color="skyblue")
            # Etiquetas y título
            ax.set_xlabel("Nivel")
            ax.set_ylabel("Cantidad de Evaluaciones")
            ax.set_title("Distribución de Evaluaciones por Nivel")
            ax.set_xticks(range(len(nivel_counts.index)))
            ax.set_xticklabels(nivel_counts.index, rotation=45)
            
            # Mostrar el gráfico en Streamlit
            st.pyplot(fig)
            df_seguimiento= df_resultados['Evaluador'].value_counts().reset_index()
            df_seguimiento.columns = ['Evaluador', 'Evaluaciones Realizadas']
            df_seguimiento['Nº Colaboradores'] = df_seguimiento['Evaluador'].apply(
                lambda Evaluador: df_personas[df_personas["SUPERVISOR"] == Evaluador].shape[0]
            )
            df_seguimiento['% Consecución'] = df_seguimiento['Evaluaciones Realizadas'] / df_seguimiento['Nº Colaboradores'] * 100
            df_seguimiento['% Consecución'] = df_seguimiento['% Consecución'].map('{:.2f} %'.format)
            st.subheader("Seguimiento de Evaluación")
            st.table(df_seguimiento)
            excelfile2= to_excel(df_seguimiento)
            st.download_button(
                label="📥 Descargar Seguimiento",
                data=excelfile2,
                file_name="Seguimiento.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.subheader("Personas Evaluadas")
            df_Evaluadas = df_resultados[['Nombre', 'Evaluador']]
            st.table(df_Evaluadas)
            excel_file4 = to_excel(df_Evaluadas)
            st.download_button(
                label="📥 Personas Evaluadas",
                data=excel_file4,
                file_name="Personas_Evaluadas.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
             

            st.subheader("Personas por Evaluar")
            #Filtrar df_personas
            df_filtrado = df_personas if Nombre_seleccionado == "Todos" else df_personas[df_personas["Nombre"] == Nombre_seleccionado]
            df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
            df_filtrado = df_filtrado if evaluador_seleccionado == "Todos" else df_filtrado[df_filtrado["Evaluador"] == evaluador_seleccionado]
            df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]
            df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
            df_filtrado = df_filtrado if empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == empresa_seleccionada]

            dfno_evaluados = df_filtrado[~df_filtrado['Nombre'].isin(df_Evaluadas['Nombre'])][['Nombre', 'SUPERVISOR']]
            dfno_evaluados = dfno_evaluados.rename(columns={'SUPERVISOR': 'Evaluador'})
            excel_file3 = to_excel(dfno_evaluados)
            st.table(dfno_evaluados)
            st.download_button(
                label="📥 Personas No Evaluadas",
                data=excel_file3,
                file_name="Personas_No_Evaluadas.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


        if lat == 'Resultados en detalle':
            Niveles = ['Junior 0 - 2 años', 'Intermedio 2 - 4 años', 'Senior > 4 años']
        
            st.subheader('RESULTADOS EN DETALLE')
        
            # Obtener los datos solo una vez
            df_resultados = ver_datos2()
            df_valoraciones_actualizadas = ver_datos()
            df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Área</h4>", unsafe_allow_html=True)
            area_seleccionada = st.selectbox(
                'Selecciona el Área:',
                ['Todos'] + sorted(df_resultados['Área'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_valoraciones_actualizadas if area_seleccionada == "Todos" else df_valoraciones_actualizadas[df_valoraciones_actualizadas["Área"] == area_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)
 
            Departamento_seleccionado = st.selectbox(
                'Selecciona el Departamento:',
                ['Todos'] + sorted(df_filtrado['Departamento'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
            seccion_seleccionada = st.selectbox(
                'Selecciona la Sección:',
                ['Todos'] + sorted(df_filtrado['Sección'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
            Puesto_seleccionado = st.selectbox(
                'Selecciona el Puesto:',
                ['Todos'] + sorted(df_filtrado['Puesto'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)
            Empresa_seleccionada = st.selectbox(
                    'Selecciona la Empresa:',
                    ['Todos'] + sorted(df_filtrado['Empresa'].dropna().astype(str).unique().tolist()),
                    label_visibility='collapsed'
                )
 
            df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Evaluador</h4>", unsafe_allow_html=True)
            evaluador_seleccionado = st.selectbox(
                'Selecciona el Evaluador:',
                ['Todos'] + sorted(df_filtrado['Evaluador'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if evaluador_seleccionado == "Todos" else df_filtrado[df_filtrado["Evaluador"] == evaluador_seleccionado]
 
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Nombre</h4>", unsafe_allow_html=True)
            Nombre_seleccionado = st.selectbox(
                'Selecciona el Nombre:',
                ['Todos'] + sorted(df_resultados['Nombre'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Nombre_seleccionado == "Todos" else df_filtrado[df_filtrado["Nombre"] == Nombre_seleccionado]
            df_resultados= df_filtrado
            df_valoraciones_actualizadas= df_filtrado

            # Organizar las columnas
            cols = [col for col in df_valoraciones_actualizadas.columns if col != 'Fecha'] + ['Fecha']
            df_valoraciones_actualizadas = df_valoraciones_actualizadas[cols]
        
            # Mostrar la tabla solo una vez
            st.table(df_valoraciones_actualizadas)
        
            # 📥 Descargar el archivo Excel
            excel_file = to_excel(df_valoraciones_actualizadas)
            st.download_button(
                label="📥 Descargar Valoraciones",
                data=excel_file,
                file_name="Valoraciones.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )



    elif usuario_autenticado == "admin":
        lat = st.sidebar.selectbox('Menú',('Resultados','Resultados en detalle'))
        if lat == 'Resultados':
            st.title("RESULTADOS DEL PROCESO DE EVALUACIÓN DEL DESEMPEÑO")                        
            df_valoraciones_actualizadas = ver_datos()
            df_resultados = ver_datos2()
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Área</h4>", unsafe_allow_html=True)
            area_seleccionada = st.selectbox(
                'Selecciona el Área:',
                ['Todos'] + sorted(df_personas['Área'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_resultados if area_seleccionada == "Todos" else df_resultados[df_resultados["Área"] == area_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)

            Departamento_seleccionado = st.selectbox(
                'Selecciona el Departamento:',
                ['Todos'] + sorted(df_filtrado['Departamento'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
            seccion_seleccionada = st.selectbox(
                'Selecciona la Sección:',
                ['Todos'] + sorted(df_filtrado['Sección'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
            Puesto_seleccionado = st.selectbox(
                'Selecciona el Puesto:',
                ['Todos'] + sorted(df_filtrado['Puesto'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)
            Empresa_seleccionada = st.selectbox(
                    'Selecciona la Empresa:',
                    ['Todos'] + sorted(df_filtrado['Empresa'].dropna().astype(str).unique().tolist()),
                    label_visibility='collapsed'
                )
 
            df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Evaluador</h4>", unsafe_allow_html=True)
            evaluador_seleccionado = st.selectbox(
                'Selecciona el Evaluador:',
                ['Todos'] + sorted(df_filtrado['Evaluador'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if evaluador_seleccionado == "Todos" else df_filtrado[df_filtrado["Evaluador"] == evaluador_seleccionado]

            st.markdown("<h4 style='font-size: 16px;'>Filtro por Nombre</h4>", unsafe_allow_html=True)
            Nombre_seleccionado = st.selectbox(
                'Selecciona el Nombre:',
                ['Todos'] + sorted(df_resultados['Nombre'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Nombre_seleccionado == "Todos" else df_filtrado[df_filtrado["Nombre"] == Nombre_seleccionado]
            df_resultados= df_filtrado
                    
            df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
            df_resultados = df_resultados.sort_values('Valoración_Obtenida').drop_duplicates(subset=['Nombre'], keep='last')


            result_styled = df_resultados.style.applymap(highlight_cells, subset=['Diferencia_Retr'])
            st.markdown("### Rango de Niveles")

            # Crear columnas para distribuir los itinerarios en paralelo
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Itinerario 3.1")
                st.markdown("""
                - **Junior**: 0 - 2 años  
                - **Intermedio**: 2 - 4 años  
                - **Senior**: > 4 años  
                """)
            
                st.markdown("#### Itinerario 4")
                st.markdown("""
                - **Junior**: 0 - 2 años  
                - **Intermedio**: 2 - 5 años  
                - **Avanzado**: 5 - 8 años  
                - **Senior**: > 8 años  
                """)
            
            with col2:
                st.markdown("#### Itinerario 3.2")
                st.markdown("""
                - **Junior**: 0 - 3 años  
                - **Intermedio**: 3 - 6 años  
                - **Senior**: > 6 años  
                """)
            
                st.markdown("#### Itinerario 5")
                st.markdown("""
                - **Junior**: 0 - 2 años  
                - **Intermedio**: 2 - 5 años  
                - **Avanzado**: 5 - 8 años  
                - **Experto**: 8 - 10 años  
                - **Senior**: > 10 años  
                """)

            result_styled = df_resultados.sort_values('Fecha').drop_duplicates(subset=['Nombre'], keep='last')
            # Mostrar resultados
            columnas_a_mostrar = [col for col in result_styled.columns if col != "Observación"]
            st.table(result_styled[columnas_a_mostrar])
            
            excel_file2 = to_excel(df_resultados)
            
            st.download_button(
                label="📥 Descargar Resultados",
                data=excel_file2,
                file_name="Resultados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.markdown("### Observaciones")
            df_observaciones= df_resultados[["Evaluador", "Nombre", "Observación"]].dropna(subset=["Observación"])
            df_observaciones = df_observaciones[df_observaciones["Observación"].str.strip() != ""]
            excel_file6 = to_excel(df_observaciones)
            st.table(df_observaciones)
            st.download_button(
            label="📥 Descargar Observaciones",
            data=excel_file6,
            file_name="Observaciones.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
            nivel_counts = df_resultados["Nivel"].value_counts().astype(int)
            # Crear el gráfico de barras en Streamlit
            st.markdown("### Distribución de Evaluaciones por Nivel")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(nivel_counts.index, nivel_counts.values, color="skyblue")
            # Etiquetas y título
            ax.set_xlabel("Nivel")
            ax.set_ylabel("Cantidad de Evaluaciones")
            ax.set_title("Distribución de Evaluaciones por Nivel")
            ax.set_xticks(range(len(nivel_counts.index)))
            ax.set_xticklabels(nivel_counts.index, rotation=45)
            
            # Mostrar el gráfico en Streamlit
            st.pyplot(fig)
            df_seguimiento= df_resultados['Evaluador'].value_counts().reset_index()
            df_seguimiento.columns = ['Evaluador', 'Evaluaciones Realizadas']
            df_seguimiento['Nº Colaboradores'] = df_seguimiento['Evaluador'].apply(
                lambda Evaluador: df_personas[df_personas["SUPERVISOR"] == Evaluador].shape[0]
            )
            df_seguimiento['% Consecución'] = df_seguimiento['Evaluaciones Realizadas'] / df_seguimiento['Nº Colaboradores'] * 100
            df_seguimiento['% Consecución'] = df_seguimiento['% Consecución'].map('{:.2f} %'.format)
            st.subheader("Seguimiento de Evaluación")
            st.table(df_seguimiento)
            excelfile2= to_excel(df_seguimiento)
            st.download_button(
                label="📥 Descargar Seguimiento",
                data=excelfile2,
                file_name="Seguimiento.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.subheader("Personas Evaluadas")
            df_Evaluadas = df_resultados[['Nombre', 'Evaluador']]
            st.table(df_Evaluadas)
            excel_file4 = to_excel(df_Evaluadas)
            st.download_button(
                label="📥 Personas Evaluadas",
                data=excel_file4,
                file_name="Personas_Evaluadas.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
             

            st.subheader("Personas por Evaluar")
            #Filtrar df_personas
            df_filtrado = df_personas if Nombre_seleccionado == "Todos" else df_personas[df_personas["Nombre"] == Nombre_seleccionado]
            df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
            df_filtrado = df_filtrado if evaluador_seleccionado == "Todos" else df_filtrado[df_filtrado["Evaluador"] == evaluador_seleccionado]
            df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]
            df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
            df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]
            dfno_evaluados = df_filtrado[~df_filtrado['Nombre'].isin(df_Evaluadas['Nombre'])][['Nombre', 'SUPERVISOR']]
            dfno_evaluados = dfno_evaluados.rename(columns={'SUPERVISOR': 'Evaluador'})
            excel_file3 = to_excel(dfno_evaluados)
            st.table(dfno_evaluados)
            st.download_button(
                label="📥 Personas No Evaluadas",
                data=excel_file3,
                file_name="Personas_No_Evaluadas.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


        if lat == 'Resultados en detalle':
            Niveles = ['Junior 0 - 2 años', 'Intermedio 2 - 4 años', 'Senior > 4 años']
        
            st.subheader('RESULTADOS EN DETALLE')
        
            # Obtener los datos solo una vez
            df_resultados = ver_datos2()
            df_valoraciones_actualizadas = ver_datos()
            df_valoraciones_actualizadas = df_valoraciones_actualizadas.sort_values('Fecha').drop_duplicates(subset=['Nombre', 'idConocimiento'], keep='last')
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Área</h4>", unsafe_allow_html=True)
            area_seleccionada = st.selectbox(
                'Selecciona el Área:',
                ['Todos'] + sorted(df_resultados['Área'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_valoraciones_actualizadas if area_seleccionada == "Todos" else df_valoraciones_actualizadas[df_valoraciones_actualizadas["Área"] == area_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Departamento</h4>", unsafe_allow_html=True)
 
            Departamento_seleccionado = st.selectbox(
                'Selecciona el Departamento:',
                ['Todos'] + sorted(df_filtrado['Departamento'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Departamento_seleccionado == "Todos" else df_filtrado[df_filtrado["Departamento"] == Departamento_seleccionado]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Sección</h4>", unsafe_allow_html=True)
            seccion_seleccionada = st.selectbox(
                'Selecciona la Sección:',
                ['Todos'] + sorted(df_filtrado['Sección'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if seccion_seleccionada == "Todos" else df_filtrado[df_filtrado["Sección"] == seccion_seleccionada]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Puesto</h4>", unsafe_allow_html=True)
            Puesto_seleccionado = st.selectbox(
                'Selecciona el Puesto:',
                ['Todos'] + sorted(df_filtrado['Puesto'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Puesto_seleccionado == "Todos" else df_filtrado[df_filtrado["Puesto"] == Puesto_seleccionado]
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Empresa</h4>", unsafe_allow_html=True)

            Empresa_seleccionada = st.selectbox(

                    'Selecciona la Empresa:',

                    ['Todos'] + sorted(df_filtrado['Empresa'].dropna().astype(str).unique().tolist()),

                    label_visibility='collapsed'

                )
 
            df_filtrado = df_filtrado if Empresa_seleccionada == "Todos" else df_filtrado[df_filtrado["Empresa"] == Empresa_seleccionada]

            st.markdown("<h4 style='font-size: 16px;'>Filtro por Evaluador</h4>", unsafe_allow_html=True)
            evaluador_seleccionado = st.selectbox(
                'Selecciona el Evaluador:',
                ['Todos'] + sorted(df_filtrado['Evaluador'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if evaluador_seleccionado == "Todos" else df_filtrado[df_filtrado["Evaluador"] == evaluador_seleccionado]
 
            st.markdown("<h4 style='font-size: 16px;'>Filtro por Nombre</h4>", unsafe_allow_html=True)
            Nombre_seleccionado = st.selectbox(
                'Selecciona el Nombre:',
                ['Todos'] + sorted(df_resultados['Nombre'].dropna().astype(str).unique().tolist()),
                label_visibility='collapsed'
            )
            df_filtrado = df_filtrado if Nombre_seleccionado == "Todos" else df_filtrado[df_filtrado["Nombre"] == Nombre_seleccionado]
            df_resultados= df_filtrado
            df_valoraciones_actualizadas= df_filtrado
            
            # Organizar las columnas
            cols = [col for col in df_valoraciones_actualizadas.columns if col != 'Fecha'] + ['Fecha']
            df_valoraciones_actualizadas = df_valoraciones_actualizadas[cols]
        
            # Mostrar la tabla solo una vez
            st.table(df_valoraciones_actualizadas)
        
            # 📥 Descargar el archivo Excel
            excel_file = to_excel(df_valoraciones_actualizadas)
            st.download_button(
                label="📥 Descargar Valoraciones",
                data=excel_file,
                file_name="Valoraciones.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    if st.button("Cerrar sesión"):
        st.session_state.authenticated = False
        st.session_state.user = None
        st.rerun()
    footer = st.container()
    with footer:
        col1, col2 = st.columns([1, 1])
        with col2:
            st.image("logoImproven.png",  width=100)
        with col1:
            st.image("logoSklum.png", width=200)

else:
    st.title("Iniciar Sesión")
    username_input = st.text_input("Nombre de usuario")
    password_input = st.text_input("Contraseña", type="password")

    if st.button("Acceder"):
        if autenticar_usuario(username_input, password_input):
            st.session_state.authenticated = True
            st.session_state.user = username_input
            st.rerun()  # Recargar para mostrar el contenido protegido
        else:
            st.error("Nombre de usuario o contraseña incorrectos. Intenta de nuevo.")
    st.write("<br>" * 14, unsafe_allow_html=True)
    footer = st.container()

    with footer:
        col1, col2 = st.columns([1, 1])
        with col2:
            st.image("logoImproven.png",  width=100)
        with col1:
            st.image("logoSklum.png", width=200)
