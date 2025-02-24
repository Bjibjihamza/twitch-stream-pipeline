import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
from datetime import datetime, timedelta
import time

# Configuration de la page
st.set_page_config(
    page_title="Analyse des Streams",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Fonction pour se connecter à la base de données MySQL
@st.cache_resource
def init_connection():
    return mysql.connector.connect(
        host="localhost",
        user="hamzabji",
        password="4753",  # Remplacez par votre mot de passe
        database="streamers"
    )

# Fonction pour exécuter des requêtes SQL
@st.cache_data(ttl=600)
def run_query(query):
    conn = init_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

# Fonction pour charger les données depuis MySQL
@st.cache_data(ttl=600)
def load_data():
    # Requête pour les catégories
    categories_query = """
    SELECT 
        category, 
        AVG(viewers) as avg_viewers, 
        MAX(viewers) as max_viewers,
        MIN(viewers) as min_viewers,
        COUNT(*) as count,
        GROUP_CONCAT(DISTINCT tags SEPARATOR ', ') as all_tags,
        MAX(image_url) as image_url
    FROM categories
    GROUP BY category
    ORDER BY avg_viewers DESC
    """
    
    # Requête pour les streamers
    streamers_query = """
    SELECT 
        channel,
        category,
        AVG(viewers) as avg_viewers,
        MAX(viewers) as max_viewers,
        COUNT(*) as stream_count,
        GROUP_CONCAT(DISTINCT tags SEPARATOR ', ') as tags
    FROM streamers
    GROUP BY channel, category
    ORDER BY avg_viewers DESC
    """
    
    # Requête pour les données temporelles
    time_query = """
    SELECT 
        DATE(timestamp) as date,
        HOUR(timestamp) as hour,
        COUNT(*) as stream_count,
        SUM(viewers) as total_viewers,
        AVG(viewers) as avg_viewers
    FROM streamers
    GROUP BY DATE(timestamp), HOUR(timestamp)
    ORDER BY date, hour
    """
    
    # Exécuter les requêtes
    categories_data = run_query(categories_query)
    streamers_data = run_query(streamers_query)
    time_data = run_query(time_query)
    
    # Convertir en DataFrame
    categories_columns = ['category', 'avg_viewers', 'max_viewers', 'min_viewers', 'count', 'tags', 'image_url']
    streamers_columns = ['channel', 'category', 'avg_viewers', 'max_viewers', 'stream_count', 'tags']
    time_columns = ['date', 'hour', 'stream_count', 'total_viewers', 'avg_viewers']
    
    categories_df = pd.DataFrame(categories_data, columns=categories_columns)
    streamers_df = pd.DataFrame(streamers_data, columns=streamers_columns)
    time_df = pd.DataFrame(time_data, columns=time_columns)
    
    return categories_df, streamers_df, time_df

# Style CSS personnalisé
st.markdown("""
    <style>
    .main {
        background-color: #f5f5f5;
    }
    .stApp {
        max-width: 1200px;
        margin: 0 auto;
    }
    h1, h2, h3 {
        color: #2C3E50;
    }
    .metric-card {
        background-color: white;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }
    .category-card {
        display: flex;
        background-color: #272530a3;;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 15px;
        align-items: center;
    }
    .category-image {
        width: 80px;
        height: 100px;
        object-fit: cover;
        margin-right: 15px;
        border-radius: 5px;
    }
    .category-info {
        flex-grow: 1;
    }
    .tag-pill {
        display: inline-block;
        background-color: #272530;
        border-radius: 15px;
        padding: 3px 10px;
        margin: 3px;
        font-size: 0.8em;
    }
    </style>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("Navigation")
section = st.sidebar.radio(
    "Choisissez une section:",
    ["Vue d'ensemble", "Catégories", "Streamers", "Analyse temporelle"]
)

# Charger les données
with st.spinner("Chargement des données..."):
    try:
        categories_df, streamers_df, time_df = load_data()
        data_loaded = True
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données: {e}")
        st.info("Veuillez vérifier vos paramètres de connexion MySQL dans le code.")
        data_loaded = False

if data_loaded:
    # En-tête principal
    st.title(" Dashboard d'analyse de streaming")
    st.markdown("---")
    
    # Vue d'ensemble
    if section == "Vue d'ensemble":
        st.header("Vue d'ensemble des données de streaming")
        
        # Métriques clés
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric(
                label="Nombre total de catégories", 
                value=f"{len(categories_df)}"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric(
                label="Nombre total de streamers uniques", 
                value=f"{streamers_df['channel'].nunique()}"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            total_viewers = categories_df['avg_viewers'].sum()
            st.metric(
                label="Total des spectateurs moyens", 
                value=f"{int(total_viewers):,}"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Top catégories
        st.subheader("Top 10 des catégories par nombre de spectateurs")
        top_categories = categories_df.sort_values('avg_viewers', ascending=False).head(10)
        
        fig = px.bar(
            top_categories,
            x='category',
            y='avg_viewers',
            color='avg_viewers',
            color_continuous_scale='Blues',
            labels={'avg_viewers': 'Nombre moyen de spectateurs', 'category': 'Catégorie'},
            title='Top 10 des catégories les plus populaires'
        )
        
        fig.update_layout(
            xaxis_title="Catégorie",
            yaxis_title="Nombre moyen de spectateurs",
            height=500,
            xaxis={'categoryorder':'total descending'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Distribution des tags
        st.subheader("Distribution des tags populaires")
        
        # Extraire et compter tous les tags
        all_tags = []
        for tags in categories_df['tags'].dropna():
            if tags:
                all_tags.extend([tag.strip() for tag in tags.split(',')])
        
        tags_count = pd.Series(all_tags).value_counts().head(20)
        
        fig = px.pie(
            names=tags_count.index,
            values=tags_count.values,
            title='Distribution des 20 tags les plus populaires',
            hole=0.4
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Rapport entre nombre de streams et viewers
        st.subheader("Rapport entre le nombre de streams et les spectateurs par catégorie")
        
        fig = px.scatter(
            categories_df.head(30),
            x='count',
            y='avg_viewers',
            color='avg_viewers',
            color_continuous_scale='Blues',
            hover_name='category',
            labels={'count': 'Nombre de streams', 'avg_viewers': 'Spectateurs moyens'},
            title='Rapport entre popularité et nombre de streams (top 30 catégories)'
        )

        # Définir une taille fixe pour tous les points
        fig.update_traces(marker=dict(size=12))
        
        fig.update_layout(height=600)
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Catégories
    elif section == "Catégories":
        st.header("Analyse détaillée des catégories")
        
        # Filtre pour la recherche de catégories
        search_term = st.text_input("Rechercher une catégorie:", "")
        
        if search_term:
            filtered_categories = categories_df[categories_df['category'].str.contains(search_term, case=False)]
        else:
            filtered_categories = categories_df
        
        # Tri des catégories
        sort_option = st.selectbox(
            "Trier par:",
            ["Spectateurs moyens (décroissant)", "Spectateurs moyens (croissant)", "Nombre de streams (décroissant)", "Alphabétique"]
        )
        
        if sort_option == "Spectateurs moyens (décroissant)":
            filtered_categories = filtered_categories.sort_values('avg_viewers', ascending=False)
        elif sort_option == "Spectateurs moyens (croissant)":
            filtered_categories = filtered_categories.sort_values('avg_viewers', ascending=True)
        elif sort_option == "Nombre de streams (décroissant)":
            filtered_categories = filtered_categories.sort_values('count', ascending=False)
        else:  # Alphabétique
            filtered_categories = filtered_categories.sort_values('category')
        
        # Afficher les catégories sous forme de cartes
        for i, row in filtered_categories.head(20).iterrows():
            st.markdown(f"""
                <div class="category-card">
                    <img src="{row['image_url']}" class="category-image" onerror="this.onerror=null;this.src='https://static-cdn.jtvnw.net/ttv-static/404_boxart-285x380.jpg';">
                    <div class="category-info">
                        <h3>{row['category']}</h3>
                        <p><strong>Spectateurs moyens:</strong> {int(row['avg_viewers']):,}</p>
                        <p><strong>Spectateurs max:</strong> {int(row['max_viewers']):,}</p>
                        <p><strong>Nombre de streams:</strong> {int(row['count'])}</p>
                        <div>
                            {"".join([f'<span class="tag-pill">{tag.strip()}</span>' for tag in row['tags'].split(',')[:5] if tag.strip()]) if row['tags'] else ""}
                        </div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        # Afficher les tags populaires
        st.subheader("Tags populaires par catégorie")
        
        # Créer une matrice de tags
        top_categories = categories_df.head(15)['category'].tolist()
        tags_by_category = {}
        
        for i, row in categories_df.iterrows():
            if row['category'] in top_categories and row['tags']:
                tags_by_category[row['category']] = [tag.strip() for tag in row['tags'].split(',')]
        
        # Créer un graphique de chaleur pour les tags
        unique_tags = set()
        for tags in tags_by_category.values():
            unique_tags.update(tags)
        
        top_tags = list(unique_tags)[:20]  # Limiter à 20 tags pour la lisibilité
        
        # Créer une matrice de présence de tags
        heatmap_data = []
        for category in top_categories:
            if category in tags_by_category:
                row_data = [1 if tag in tags_by_category[category] else 0 for tag in top_tags]
                heatmap_data.append(row_data)
            else:
                heatmap_data.append([0] * len(top_tags))
        
        # Créer le graphique de chaleur
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data,
            x=top_tags,
            y=top_categories,
            colorscale='Blues'
        ))
        
        fig.update_layout(
            title='Présence des tags populaires par catégorie',
            xaxis_title='Tags',
            yaxis_title='Catégories',
            height=600
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Streamers
    elif section == "Streamers":
        st.header("Analyse des streamers")
        
        # Filtres pour les streamers
        col1, col2 = st.columns(2)
        
        with col1:
            search_streamer = st.text_input("Rechercher un streamer:", "")
        
        with col2:
            category_filter = st.selectbox(
                "Filtrer par catégorie:",
                ["Toutes les catégories"] + sorted(categories_df['category'].tolist())
            )
        
        # Appliquer les filtres
        filtered_streamers = streamers_df.copy()
        
        if search_streamer:
            filtered_streamers = filtered_streamers[filtered_streamers['channel'].str.contains(search_streamer, case=False)]
        
        if category_filter != "Toutes les catégories":
            filtered_streamers = filtered_streamers[filtered_streamers['category'] == category_filter]
        
        # Top streamers
        st.subheader("Top 20 des streamers")
        
        top_streamers = filtered_streamers.sort_values('avg_viewers', ascending=False).head(20)
        
        fig = px.bar(
            top_streamers,
            x='channel',
            y='avg_viewers',
            color='category',
            labels={'avg_viewers': 'Spectateurs moyens', 'channel': 'Streamer', 'category': 'Catégorie'},
            title='Top 20 des streamers par nombre moyen de spectateurs'
        )
        
        fig.update_layout(
            xaxis_title="Streamer",
            yaxis_title="Spectateurs moyens",
            height=600,
            xaxis={'categoryorder':'total descending'},
            xaxis_tickangle=-45
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Distribution des streamers par catégorie
        st.subheader("Distribution des streamers par catégorie")
        
        streamers_by_category = filtered_streamers.groupby('category').size().reset_index(name='count')
        streamers_by_category = streamers_by_category.sort_values('count', ascending=False).head(15)
        
        fig = px.pie(
            streamers_by_category,
            names='category',
            values='count',
            title='Distribution des streamers par catégorie (top 15)',
            hole=0.4
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Tableau détaillé des streamers
        st.subheader("Tableau détaillé des streamers")
        
        st.dataframe(
            filtered_streamers.sort_values('avg_viewers', ascending=False),
            height=400,
            use_container_width=True
        )
    
    # Analyse temporelle
    elif section == "Analyse temporelle":
        st.header("Analyse temporelle des streams")
        
        # Convertir la colonne date si nécessaire
        if 'date' in time_df.columns:
            time_df['date'] = pd.to_datetime(time_df['date'])
        
        # Filtres temporels
        date_range = st.date_input(
            "Sélectionner une plage de dates:",
            [time_df['date'].min(), time_df['date'].max()]
        )
        
        start_date, end_date = date_range
        filtered_time_df = time_df[(time_df['date'] >= pd.Timestamp(start_date)) & 
                                 (time_df['date'] <= pd.Timestamp(end_date))]
        
        # Agrégations temporelles
        daily_data = filtered_time_df.groupby('date').agg({
            'stream_count': 'sum',
            'total_viewers': 'sum',
            'avg_viewers': 'mean'
        }).reset_index()
        
        hourly_data = filtered_time_df.groupby('hour').agg({
            'stream_count': 'mean',
            'total_viewers': 'mean',
            'avg_viewers': 'mean'
        }).reset_index()
        
        # Évolution temporelle
        st.subheader("Évolution du nombre de streams et de spectateurs")
        
        metric_option = st.selectbox(
            "Choisir la métrique à afficher:",
            ["Nombre de streams", "Total des spectateurs", "Moyenne des spectateurs"]
        )
        
        if metric_option == "Nombre de streams":
            y_col = 'stream_count'
            y_title = "Nombre de streams"
        elif metric_option == "Total des spectateurs":
            y_col = 'total_viewers'
            y_title = "Total des spectateurs"
        else:
            y_col = 'avg_viewers'
            y_title = "Moyenne des spectateurs"
        
        fig = px.line(
            daily_data,
            x='date',
            y=y_col,
            labels={'date': 'Date', y_col: y_title},
            title=f'Évolution temporelle: {y_title}',
            markers=True
        )
        
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=y_title,
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Distribution par heure de la journée
        st.subheader("Distribution par heure de la journée")
        
        fig = px.bar(
            hourly_data,
            x='hour',
            y=y_col,
            labels={'hour': 'Heure de la journée', y_col: y_title},
            title=f'Distribution par heure: {y_title}',
            color=y_col,
            color_continuous_scale='Blues'
        )
        
        fig.update_layout(
            xaxis_title="Heure",
            yaxis_title=y_title,
            height=500,
            xaxis=dict(tickmode='linear', tick0=0, dtick=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Heatmap jour/heure
        st.subheader("Heatmap de l'activité par jour et heure")
        
        # Préparer les données pour le heatmap
        if 'date' in filtered_time_df.columns and 'hour' in filtered_time_df.columns:
            filtered_time_df['day_of_week'] = filtered_time_df['date'].dt.day_name()
            
            # Ordonner les jours de la semaine
            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            
            # Créer un pivot pour le heatmap
            heatmap_data = filtered_time_df.pivot_table(
                values=y_col,
                index='day_of_week',
                columns='hour',
                aggfunc='mean'
            )
            
            # Réordonner les jours
            heatmap_data = heatmap_data.reindex(days_order)
            
            # Créer le heatmap
            fig = px.imshow(
                heatmap_data,
                labels=dict(x="Heure de la journée", y="Jour de la semaine", color=y_title),
                x=heatmap_data.columns,
                y=heatmap_data.index,
                color_continuous_scale='Blues',
                aspect="auto"
            )
            
            fig.update_layout(
                title=f'Activité moyenne ({y_title}) par jour et heure',
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)

# Footer
st.markdown("---")
st.caption("Dashboard créé avec Streamlit • Données de streaming extraites de MySQL")