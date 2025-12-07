import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Titre principal
st.title("📊 Dashboard d'Analyse des Cours en Ligne")

# Lecture du fichier CSV
df = pd.read_csv('data/processed/clean_students.csv')

# Afficher les premières lignes
# st.write(df.head())

df['score_moyen'] = df[['math score', 'reading score', 'writing score']].mean(axis=1).round(2)

df_filtered = df


# Métriques principales
st.header("📈 Indicateurs Clés")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Apprenants", len(df_filtered))

with col2:
    st.metric("Score Moyen Global", f"{df_filtered['score_moyen'].mean():.1f}")

with col3:
    st.metric("Heures Moyennes", f"{df_filtered['hours_studied'].mean():.0f}h")

with col4:
    st.metric("Taux Préparation", f"{(df_filtered['test preparation course'] == 'completed').sum() / len(df_filtered) * 100:.1f}%")

with col5:
    st.metric("Âge Moyen", f"{df_filtered['student_age'].mean():.0f} ans")

st.divider()


# Section 1: Analyse des performances
st.header("🎯 Analyse des Performances")

col1, col2 = st.columns(2)

with col1:
    # Distribution des scores par matière
    fig1 = go.Figure()
    fig1.add_trace(go.Box(y=df_filtered['math score'], name='Mathématiques', marker_color='#FF6B6B'))
    fig1.add_trace(go.Box(y=df_filtered['reading score'], name='Lecture', marker_color='#4ECDC4'))
    fig1.add_trace(go.Box(y=df_filtered['writing score'], name='Écriture', marker_color='#45B7D1'))
    fig1.update_layout(title='Distribution des Scores par Matière', yaxis_title='Score')
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    # Impact de la préparation sur les scores
    prep_scores = df_filtered.groupby('test preparation course')[['math score', 'reading score', 'writing score']].mean()
    fig2 = go.Figure(data=[
        go.Bar(name='Mathématiques', x=prep_scores.index, y=prep_scores['math score'], marker_color='#FF6B6B'),
        go.Bar(name='Lecture', x=prep_scores.index, y=prep_scores['reading score'], marker_color='#4ECDC4'),
        go.Bar(name='Écriture', x=prep_scores.index, y=prep_scores['writing score'], marker_color='#45B7D1')
    ])
    fig2.update_layout(title='Impact de la Préparation sur les Scores', barmode='group')
    st.plotly_chart(fig2, use_container_width=True)

st.divider()


# Section 2: Analyse démographique
st.header("👥 Analyse Démographique")

col1, col2 = st.columns(2)

with col1:
    # Répartition par genre et niveau d'études
    genre_niveau = df_filtered.groupby(['student_gender', 'Education Level']).size().reset_index(name='count')
    fig3 = px.bar(genre_niveau, x='Education Level', y='count', color='student_gender',
                  title='Répartition par Genre et Niveau d\'Études',
                  labels={'count': 'Nombre d\'Apprenants', 'Education Level': 'Niveau d\'Études'},
                  color_discrete_map={'Male': '#667BC6', 'Female': '#E99BBB'})
    fig3.update_xaxes(tickangle=45)
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    # Distribution des âges
    fig4 = px.histogram(df_filtered, x='student_age', nbins=20, 
                        title='Distribution des Âges',
                        labels={'student_age': 'Âge', 'count': 'Nombre d\'Apprenants'},
                        color_discrete_sequence=['#667BC6'])
    st.plotly_chart(fig4, use_container_width=True)

st.divider()


st.header("💻 Analyse Technologique")

col1, col2, col3 = st.columns(3)

with col1:
    # Répartition des équipements
    equipement_counts = df_filtered['Device'].value_counts()
    fig5 = px.pie(values=equipement_counts.values, names=equipement_counts.index,
                  title='Répartition des Équipements',
                  color_discrete_sequence=px.colors.qualitative.Set3)
    st.plotly_chart(fig5, use_container_width=True)

with col2:
    # Répartition du type d'internet
    internet_counts = df_filtered['Internet Type'].value_counts()
    fig6 = px.pie(values=internet_counts.values, names=internet_counts.index,
                  title='Type de Connexion Internet',
                  color_discrete_sequence=px.colors.qualitative.Pastel)
    st.plotly_chart(fig6, use_container_width=True)

with col3:
    # Répartition des sources
    source_counts = df_filtered['source_file'].value_counts()
    fig7 = px.pie(values=source_counts.values, names=source_counts.index,
                  title='Sources de Données',
                  color_discrete_sequence=px.colors.qualitative.Bold)
    st.plotly_chart(fig7, use_container_width=True)

# Performance par équipement
fig8 = px.box(df_filtered, x='Device', y='score_moyen', color='Device',
              title='Performance Moyenne par Équipement',
              labels={'score_moyen': 'Score Moyen', 'Device': 'Équipement'},
              color_discrete_sequence=px.colors.qualitative.Set2)
st.plotly_chart(fig8, use_container_width=True)

st.divider()


# Section 4: Analyse de l'engagement
st.header("⏱️ Analyse de l'Engagement")

col1, col2 = st.columns(2)

with col1:
    # Relation heures suivies et scores
    fig9 = px.scatter(df_filtered, x='hours_studied', y='score_moyen',
                     color='course_name', size='student_age',
                     title='Relation Heures Suivies vs Performance',
                     labels={'hours_studied': 'Heures Suivies', 'score_moyen': 'Score Moyen'},
                     trendline='ols')
    st.plotly_chart(fig9, use_container_width=True)

with col2:
    # Heures moyennes par niveau d'études
    heures_niveau = df_filtered.groupby('course_name')['hours_studied'].mean().sort_values(ascending=False)
    fig10 = px.bar(x=heures_niveau.index, y=heures_niveau.values,
                   title='Heures Moyennes par Cours',
                   labels={'x': 'Cours', 'y': 'Heures Moyennes'},
                   color=heures_niveau.values,
                   color_continuous_scale='Viridis')
    fig10.update_xaxes(tickangle=45)
    st.plotly_chart(fig10, use_container_width=True)

st.divider()

