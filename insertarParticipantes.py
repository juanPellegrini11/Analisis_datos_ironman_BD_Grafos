from pandas import read_csv
from neo4j import GraphDatabase
import time

# Conteo del tiempo para medir cuanto demora el procesamiento
start_time = time.time()

uri = "bolt://localhost:7687"
userName = "neo4j" 
password = "rootroot"

graphDB_Driver = GraphDatabase.driver(uri, auth=(userName, password))
df_con_cupos = '/Users/juanpellegrini/Documents/Maestría/df_con_cupos.csv'

with graphDB_Driver.session() as graphDB_Session:

    df_completo = read_csv(df_con_cupos, low_memory=False)

    largo = len(df_completo.index) - 1
    for idx in range(0,largo): 

        row_df_completo = df_completo.iloc[[idx]]

        genero = str(row_df_completo.Gender.values[0])
        ageGroup = str(row_df_completo.AgeGroup.values[0])
        banda_categoria = str(row_df_completo.AgeBand.values[0])
        swimTime = str(row_df_completo.SwimTime.values[0])
        transition1Time = str(row_df_completo.Transition1Time.values[0])
        bikeTime = str(row_df_completo.BikeTime.values[0])
        transition2Time = str(row_df_completo.Transition2Time.values[0])
        runTime = str(row_df_completo.RunTime.values[0])
        finishTime = str(row_df_completo.FinishTime.values[0])
        eventYear = str(row_df_completo.EventYear.values[0])
        eventLocation = str(row_df_completo.EventLocation.values[0])
        lugar = str(row_df_completo.Lugar.values[0])
        country = str(row_df_completo.Country.values[0])
        clasifica = str(row_df_completo.clasifica.values[0])
        categoria = str(row_df_completo.categoria.values[0])

        crear_participante  = "CREATE (p: Participante {Genero: $genero, Age_Group: $ageGroup, Banda_Categoria: $banda_categoria, Categoria: $categoria, SwimTime: $swimTime, Transition1Time: $transition1Time, BikeTime: $bikeTime, Transition2Time: $transition2Time, RunTime: $runTime, FinishTime: $finishTime, Clasifica: $clasifica}) "
        relacion_pais_origen = "MERGE (n: Pais {Nombre_Pais: $country}) MERGE (p)-[w:ES_DE]-(n) "
        relacion_evento = "MERGE (e: Evento {Nombre_Evento:  $eventLocation, Año: $eventYear}) MERGE (p)-[s:PARTICIPO_EN]-(e) "
        relacion_lugar = "MERGE (l: Lugar {Nombre_Lugar:  $lugar}) MERGE (e)-[t:SE_CORRE_EN]-(l)"
      
        query = crear_participante + relacion_pais_origen + relacion_evento + relacion_lugar
        graphDB_Session.run(query, genero = genero, ageGroup = ageGroup, banda_categoria = banda_categoria, categoria = categoria, swimTime = swimTime,  transition1Time = transition1Time, bikeTime = bikeTime, transition2Time = transition2Time, runTime =runTime, finishTime = finishTime,  eventYear = eventYear, country = country, eventLocation= eventLocation, lugar = lugar, clasifica = clasifica)

# Conteo del tiempo final
end_time = time.time()

# Se imprime el tiempo total
execution_time = end_time - start_time
print('Tiempo de ejecucion de la funcion', execution_time)