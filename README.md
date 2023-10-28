# Ironman_analysis_and_graph_database

El presente trabajo se realiza con el motivo de estudiar las competencias de Ironman 70.3 en el mundo. Para ello se utiliza el dataset del siguiente link https://www.kaggle.com/datasets/aiaiaidavid/ironman-703-race-data-between-2004-and-2020?resource=download
El mismo se le realiza un análisis previo y se modifica agregándole información de interés, luego se crea una base de datos de grafos para su estudio.

## Análisis previo

Para ello se realizan las operaciones del script "Limpieza_y_paises.ipynb" de este repositorio.
También se complementa el mismo agregando información de clasificación a mundiales utilizando el script "cupos.R"

## Base de datos de grafos en neo4j

Para ello se utilizan los comandos utilizados en "crearBaseDeDatos.txt" donde se crean los países, lugares y eventos según se muestra en el archivo.
Luego se insertan los participantes que, por ser un gran volumen de datos, se realiza utilizando el script hecho en python "insertarParticipantes.py"

## Análisis de datos

Para el análisis de los datos se utiliza el script "analisis_resultados.py" donde se generan los dataframes que se pueden ver en la respectiva carpeta. 
