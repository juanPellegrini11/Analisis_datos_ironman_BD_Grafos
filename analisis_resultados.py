#%%
!pip3 install matplotlib
!pip3 install seaborn

#%%
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns
from neo4j import GraphDatabase
import numpy as np
#%%
uri = "bolt://localhost:7687"
userName = "neo4j" 
password = "rootroot"

graphDB_Driver = GraphDatabase.driver(uri, auth=(userName, password))

#%%
with graphDB_Driver.session() as session:

    query = '''MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE p1.Nombre_Pais = p2.Nombre_Pais AND pa.Clasifica = '1.0'
                WITH p1, count(*) AS total_residentes_clasificados
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE p1.Nombre_Pais = p2.Nombre_Pais
                WITH p2, count(*) AS total_residentes, total_residentes_clasificados
                RETURN total_residentes_clasificados, total_residentes, toFloat(total_residentes_clasificados)/toFloat(total_residentes)*100 AS ratio, p2.Nombre_Pais AS pais_evento
                ORDER BY ratio DESC'''  
    residentes_clasificados_sobre_total_residentes = pd.DataFrame(session.run(query).data())


#%%
with graphDB_Driver.session() as session:

    query = '''MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WITH p2, count(*) AS total_competidores
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE p1 <> p2
                WITH p2, count(*) AS total_extranjeros, total_competidores
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE pa.Clasifica = '1.0'
                WITH p2, count(*) AS total_Clasificados, total_competidores, total_extranjeros
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE p1 <> p2 and pa.Clasifica = '1.0'
                WITH p2, count(*) AS total_extranjeros_clasificados, p1, total_Clasificados, toFloat(count(*))/toFloat(total_Clasificados)*100 AS ratio_clasificados_extranjeros,total_competidores, toFloat(total_extranjeros)/toFloat(total_competidores)*100 AS ratio_extranjeros
                RETURN p1.Nombre_Pais AS Pais_Procedencia, total_competidores, total_Clasificados, total_extranjeros_clasificados, ratio_clasificados_extranjeros, ratio_extranjeros, ratio_clasificados_extranjeros/ratio_extranjeros AS ratio1_sobre_ratio2, p2.Nombre_Pais AS Pais_Evento
                ORDER BY ratio_clasificados_extranjeros desc'''
    
    total_extranjeros_clasificados_sobre_total_clasificados = pd.DataFrame(session.run(query).data())

#%%
with graphDB_Driver.session() as session:

    query = '''MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WITH p2, count(*) AS total_competidores
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE p1 = p2
                WITH p2, count(*) AS total_residentes, total_competidores
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE pa.Clasifica = '1.0'
                WITH p2, count(*) AS total_clasificados, total_competidores, total_residentes
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE p1.Nombre_Pais = p2.Nombre_Pais and pa.Clasifica = '1.0'
                WITH p2, count(*) AS total_residentes_clasificados, total_clasificados, total_competidores, total_residentes, toFloat(count(*))/toFloat(total_clasificados)*100 AS ratio_clasificados_residentes, toFloat(total_residentes)/toFloat(total_competidores)*100 AS ratio_cantidad_residentes
                RETURN p2.Nombre_Pais AS pais_evento, total_competidores, total_residentes, total_residentes_clasificados, total_clasificados, ratio_clasificados_residentes, ratio_cantidad_residentes, ratio_clasificados_residentes/ratio_cantidad_residentes AS relacion_ratio1_sobre_ratio2
                ORDER BY relacion_ratio1_sobre_ratio2 DESC
                '''
    
    ratio_residentes_clasificados_vs_ratio_cantidad_participantes = pd.DataFrame(session.run(query).data())

#%%
with graphDB_Driver.session() as session:

    query = '''MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WITH p2, count(*) AS total_competidores
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE pa.Clasifica = '1.0'
                WITH p2, count(*) AS total_clasificados, total_competidores
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WITH p2, p1, count(*) AS competidores_por_pais_procedencia, total_clasificados, total_competidores
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE pa.Clasifica = '1.0'
                WITH p2, p1, count(*) AS clasificados_por_pais, total_clasificados, total_competidores, competidores_por_pais_procedencia, toFloat(count(*))/toFloat(total_clasificados)*100 AS ratio_clasificados, toFloat(competidores_por_pais_procedencia)/toFloat(total_competidores)*100 AS ratio_competidores
                RETURN p2.Nombre_Pais AS pais_evento, total_competidores, competidores_por_pais_procedencia, total_clasificados, clasificados_por_pais, ratio_clasificados, ratio_competidores, ratio_clasificados/ratio_competidores AS ratio1_sobre_ratio2, p1.Nombre_Pais AS pais_procedencia'''

    ratio_clasificaciones_por_pais_vs_ratio_cantidad_participantes = pd.DataFrame(session.run(query).data())

#%%
with graphDB_Driver.session() as session:

    query = '''MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WITH p1, count(*) AS participantes_por_pais
                MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE pa.Clasifica = '1.0'
                WITH p1, count(*) AS clasificados_por_pais, participantes_por_pais, toFloat(count(*))/toFloat(participantes_por_pais)*100 AS clasificados_sobre_cantidad_participantes
                RETURN p1.Nombre_Pais AS pais_procedencia, participantes_por_pais, clasificados_sobre_cantidad_participantes
                ORDER BY clasificados_sobre_cantidad_participantes DESC'''
    
    clasificados_por_pais = pd.DataFrame(session.run(query).data())

#%%
with graphDB_Driver.session() as session:
    
    query1 = '''MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WITH p2.Nombre_Pais AS pais_evento, l.Nombre_Lugar AS lugar, l.Latitud AS latitud, e.Año AS año, pa.Genero AS genero, pa.Age_Group AS categoria, AVG(toFloat(pa.FinishTime)/3600) AS prom_finish_time
                RETURN pais_evento, lugar, latitud, año, genero, categoria, prom_finish_time'''
    
    query2 = '''MATCH (p1:Pais)<-[:ES_DE]-(pa:Participante)-[:PARTICIPO_EN]->(e:Evento)-[:SE_CORRE_EN]->(l:Lugar)-[:PERTENECE]->(p2:Pais)
                WHERE pa.Clasifica = '1.0'
                WITH p2.Nombre_Pais AS pais_evento, l.Nombre_Lugar AS lugar, l.Latitud AS latitud, e.Año AS año, pa.Genero AS genero, pa.Age_Group AS categoria, MAX(toFloat(pa.FinishTime)/3600) AS max_tiempo_clasificacion
                RETURN pais_evento, lugar, latitud, año, genero, categoria, max_tiempo_clasificacion'''
    
    promedios_por_genero_evento_edad_anio = pd.DataFrame(session.run(query1).data())
    max_tiempos_clasificacion_por_genero_evento_edad_anio = pd.DataFrame(session.run(query2).data())

#%%
clasificados_por_genero_evento_edad_anio = pd.merge(max_tiempos_clasificacion_por_genero_evento_edad_anio, promedios_por_genero_evento_edad_anio, on=['genero', 'categoria', 'lugar', 'año'])
clasificados_por_genero_evento_edad_anio['ratio'] = clasificados_por_genero_evento_edad_anio['prom_finish_time'] - clasificados_por_genero_evento_edad_anio['max_tiempo_clasificacion']
clasificados_por_genero_evento_edad_anio = clasificados_por_genero_evento_edad_anio.drop(['latitud_y', 'pais_evento_y'], axis=1)
clasificados_por_genero_evento_edad_anio = clasificados_por_genero_evento_edad_anio[clasificados_por_genero_evento_edad_anio['ratio'] > 0]
clasificados_por_genero_evento_edad_anio = clasificados_por_genero_evento_edad_anio.sort_values(by=['genero', 'categoria', 'año', 'ratio'], ascending=[False, True, False, True])


#%%
def show_values(axs, orient="v", space=.01):
    '''Funcion auxiliar para ajustar valores de grafico de barras'''
    def _single(ax):
        if orient == "v":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 2
                _y = p.get_y() + p.get_height() + (p.get_height()*0.01)
                value = '{:.1f}'.format(p.get_height())
                ax.text(_x, _y, value, ha="center") 
        elif orient == "h":
            for p in ax.patches:
                _x = p.get_x() + p.get_width() + float(space)
                _y = p.get_y() + p.get_height() - (p.get_height()*0.2)
                value = '{:.1f}'.format(p.get_width())
                ax.text(_x, _y, value, ha="left")

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _single(ax)
    else:
        _single(axs)
#%%
def grafico_barras_horizontales(datos, ejeX, ejeY, xLabel, yLabel, titulo, tamanioGraf):
    '''Funcion para crear grafico de barras horizontal'''
    f, ax = plt.subplots(figsize=tamanioGraf)
    plot = sns.barplot(data = datos, x = ejeX, y = ejeY, orient = 'horizontal')
    plot.set(xlabel = xLabel, ylabel = yLabel, title = titulo)
    for i, label in enumerate(plot.get_yticklabels()):
        if label.get_text() == 'Uruguay':
            label.set_color('red')
            label.set_fontweight('bold')
            bars = ax.containers[0]
            bars[i].set_facecolor('red')
    show_values(plot, "h", space=0)
    plt.legend()
    plt.tight_layout()
    return plt

#%%
# %Total de clasificados / cantidad de participantes por pais
plt = grafico_barras_horizontales(clasificados_por_pais[clasificados_por_pais['participantes_por_pais'] > 200].sort_values(by='clasificados_sobre_cantidad_participantes', ascending=False), 'clasificados_sobre_cantidad_participantes', 'pais_procedencia', 'clasificados/cantidad_participantes', 'País de procedencia', '% Total de clasificados por país', (6, 20))
plt.show()
#%%
#  Estudio de residentes
plt = grafico_barras_horizontales(ratio_residentes_clasificados_vs_ratio_cantidad_participantes, 'relacion_ratio1_sobre_ratio2', 'pais_evento', 'ratio', 'País', 'Ratio clasificados residentes / Ratio cantidad residentes', (6, 20))
plt.axvline(x=1, color='red', linestyle='--', label = 'lim.rel > 1')
plt.legend()
plt.show()

#%%
# Estudio de los 10 mejores paises clasificando en cada pais de evento
top_10_clasificaciones = ratio_clasificaciones_por_pais_vs_ratio_cantidad_participantes.groupby('pais_evento').apply(lambda x: x.nlargest(10, 'ratio1_sobre_ratio2')).reset_index(drop=True)
for pais_evento in top_10_clasificaciones['pais_evento'].unique():
    grafico_barras_horizontales(top_10_clasificaciones[top_10_clasificaciones['pais_evento'] == pais_evento].sort_values(by='ratio1_sobre_ratio2', ascending=False), 'ratio1_sobre_ratio2','pais_procedencia', 'ratio1/ratio2', 'País de procedencia', 'ratio1/ratio2 por país de procedencia', (6,20))

# %%
f, ax = plt.subplots(figsize=(6, 15))
plot = sns.barplot(data = ratio_clasificaciones_por_pais_vs_ratio_cantidad_participantes.sort_values(by='ratio1_sobre_ratio2', ascending=False), x = 'ratio1_sobre_ratio2', y = 'pais_evento', orient = 'horizontal')
plot.set(xlabel ="ratio1/ratio2", ylabel = "País del evento", title ='ratio1/ratio2')
plt.axvline(x=1, color='red', linestyle='--', label = 'linea roja')
for i, label in enumerate(plot.get_yticklabels()):
    if label.get_text() == 'Uruguay':
        label.set_color('red')
        label.set_fontweight('bold')
        bars = ax.containers[0]
        bars[i].set_facecolor('red')
plt.legend()
plt.show()

# %%
# Mapa de calor Paises de evento vs paises de procedencia, en la intersección se muestra la cantidad de participantes clasificados
f, ax = plt.subplots(figsize=(100, 100))
heatmap_data = ratio_clasificaciones_por_pais_vs_ratio_cantidad_participantes.pivot_table(index='pais_procedencia', columns='pais_evento', values='ratio1_sobre_ratio2')
ax = sns.heatmap(heatmap_data, fmt='.2f', annot = True)
plt.tight_layout()
