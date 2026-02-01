import streamlit as st
import pandas as pd
import time
from datetime import datetime

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(page_title="AnalyticsEngine: Simulador", layout="wide")
st.title("🏛️ AnalyticsEngine: Simulador Wide-Column")

# --- INICIALIZACIÓN DE LA BASE DE DATOS ---
if 'cassandra_db' not in st.session_state:
    st.session_state.cassandra_db = {
        'Datos_Usuario': {},       # Familia 1 (Información Personal)
        'Datos_Geograficos': {},   # Familia 2 (Dimensiones)
        'Datos_Metricas': {}       # Familia 3 (Hechos/Métricas)
    }

# MAPA DE ESQUEMA (Diccionario de Datos)
SCHEMA_MAP = {
    "nombre": "Datos_Usuario",
    "email": "Datos_Usuario",
    "ciudad": "Datos_Geograficos",
    "pais": "Datos_Geograficos",
    "visitas": "Datos_Metricas",
    "gasto_publicitario": "Datos_Metricas", # <--- NUEVO CAMPO
    "ultima_sesion": "Datos_Metricas"
}

# --- PESTAÑAS DE NAVEGACIÓN ---
# Añadimos la tercera pestaña "📊 Analítica de Negocio"
tab_write, tab_read, tab_analytics = st.tabs(["📝 Escritura", "⚡ Consulta OLAP", "📊 Analítica de Negocio"])

# ==============================================================================
# PESTAÑA 1: ESCRITURA (Formulario Actualizado)
# ==============================================================================
with tab_write:
    col_form, col_view = st.columns([1, 2])
    
    with col_form:
        st.subheader("Insertar Registro")
        with st.form("cassandra_insert"):
            row_key = st.text_input("🔑 Row Key (ID)", placeholder="user_101")
            
            st.markdown("---")
            st.caption("Familia: Datos_Usuario")
            nombre = st.text_input("Nombre", value="Ana")
            email = st.text_input("Email", value="ana@mail.com")
            
            st.caption("Familia: Datos_Geograficos")
            ciudad = st.selectbox("Ciudad", ["Madrid", "Barcelona", "Sevilla", "Valencia", "Bilbao"])
            pais = st.text_input("País", value="España")
            
            st.caption("Familia: Datos_Metricas")
            visitas = st.number_input("Visitas", min_value=0, value=15)
            # NUEVO CAMPO PARA ANALÍTICA
            gasto = st.number_input("Gasto Publicitario (€)", min_value=0.0, value=50.5, step=0.5)
            
            if st.form_submit_button("Guardar"):
                if row_key:
                    # Escritura distribuida en familias
                    st.session_state.cassandra_db['Datos_Usuario'][row_key] = {"nombre": nombre, "email": email}
                    st.session_state.cassandra_db['Datos_Geograficos'][row_key] = {"ciudad": ciudad, "pais": pais}
                    st.session_state.cassandra_db['Datos_Metricas'][row_key] = {
                        "visitas": visitas, 
                        "gasto_publicitario": gasto,
                        "ultima_sesion": str(datetime.now())
                    }
                    st.success(f"✅ Guardado: {row_key}")
                else:
                    st.error("Falta Row Key")

    with col_view:
        st.subheader("Estado Físico del Clúster")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.caption("👤 Datos_Usuario")
            st.json(st.session_state.cassandra_db['Datos_Usuario'])
        with c2:
            st.caption("🌍 Datos_Geograficos")
            st.json(st.session_state.cassandra_db['Datos_Geograficos'])
        with c3:
            st.caption("📊 Datos_Metricas")
            st.json(st.session_state.cassandra_db['Datos_Metricas'])

# ==============================================================================
# PESTAÑA 2: CONSULTA OLAP (Ya existente)
# ==============================================================================
with tab_read:
    st.header("⚡ Motor de Consulta Columnar")
    all_columns = list(SCHEMA_MAP.keys())
    selected_cols = st.multiselect("Proyección (Columnas a leer):", options=all_columns, default=["nombre", "gasto_publicitario"])
    
    if st.button("Ejecutar Query"):
        if selected_cols:
            start_time = time.perf_counter()
            
            # Recopilar claves
            all_keys = set()
            for fam in st.session_state.cassandra_db.values():
                all_keys.update(fam.keys())
            
            result_data = []
            for key in all_keys:
                row_data = {"ID": key}
                for col in selected_cols:
                    target_family = SCHEMA_MAP[col]
                    val = st.session_state.cassandra_db[target_family].get(key, {}).get(col, None)
                    row_data[col] = val
                result_data.append(row_data)
            
            df_result = pd.DataFrame(result_data).set_index("ID")
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            # Métricas
            cols_ignored = len(all_columns) - len(selected_cols)
            st.metric("⏱️ Tiempo", f"{duration_ms:.4f} ms")
            st.metric("📉 Columnas Ignoradas", cols_ignored)
            st.dataframe(df_result)
        else:
            st.warning("Selecciona columnas.")

# ==============================================================================
# PESTAÑA 3: ANALÍTICA (LO NUEVO)
# ==============================================================================
with tab_analytics:
    st.header("📊 Analítica Agregada: Gasto por Ciudad")
    
    if st.button("🔄 Calcular ROI por Región", type="primary"):
        start_time = time.perf_counter()
        
        # --- SIMULACIÓN DEL MOTOR DE ANALÍTICA ---
        # Paso 1: Escaneo Vertical (Solo leemos 2 Familias)
        # Ignoramos completamente 'Datos_Usuario' (Nombres, Emails, etc.)
        
        # Recuperamos solo CIUDAD (Dimensión)
        raw_cities = st.session_state.cassandra_db['Datos_Geograficos']
        # Recuperamos solo GASTO (Métrica)
        raw_metrics = st.session_state.cassandra_db['Datos_Metricas']
        
        # Paso 2: Join en Memoria (MapReduce implícito)
        # Unimos solo las columnas necesarias usando la Row Key
        analytics_data = []
        all_keys = set(raw_cities.keys()).union(raw_metrics.keys())
        
        for key in all_keys:
            city = raw_cities.get(key, {}).get('ciudad', 'Desconocido')
            spend = raw_metrics.get(key, {}).get('gasto_publicitario', 0.0)
            analytics_data.append({'Ciudad': city, 'Gasto': spend})
            
        # Convertimos a DataFrame para la agregación
        df_analytics = pd.DataFrame(analytics_data)
        
        if not df_analytics.empty:
            # Paso 3: Agregación (GROUP BY)
            grouped_df = df_analytics.groupby('Ciudad')['Gasto'].sum()
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            # --- VISUALIZACIÓN ---
            col_chart, col_explanation = st.columns([2, 1])
            
            with col_chart:
                st.subheader("Inversión Total (€)")
                # Gráfico de barras nativo de Streamlit
                st.bar_chart(grouped_df)
                
            with col_explanation:
                st.metric("⏱️ Tiempo de Agregación", f"{duration_ms:.4f} ms")
                st.info("Familias Escaneadas: 2 de 3")
                st.success("""
                **¿Por qué es ultra rápido?**
                
                Para generar este gráfico, el sistema ha **ignorado** completamente la familia `Datos_Usuario`.
                
                Imagina que tienes 1 millón de usuarios. En SQL tradicional, habríamos tenido que cargar 1 millón de nombres y emails en memoria RAM inútilmente.
                
                Aquí, esos datos nunca salieron del disco. Solo cruzamos `Geograficos` con `Metricas`.
                """)
        else:
            st.warning("No hay datos suficientes. Ve a la pestaña 'Escritura' e inserta registros.")
