# Se necesita saber cuales atletas logran clasificar al mundial, pero
# está información no está en los datos encontrados, por ese motivo se
# arma este script.

# Aunque en la actualidad hubo algunos cambios sobre como se reparten los cupos, 
# se llego a la conclusión de que el siguiente metodo es una buena aproximación:
# Reparto de cupos:
#                   1°) 1 cupo para cada categoria Gender-AgeBand (con al menos 1 participante)
#                   2°) Los restantes cupos se separan según género
#                   3°) Dentro de cada género se reparter según el peso de cada AgeBand.

# La cantidad de cupos cambia evento a evento y año a año, por lo lo que para simplificar 
# se supone  40 cupos para eventos nacionales y 80 para campeonates continentales.


library(data.table)
library(dplyr)

df <- fread("df_completo.csv")


# Hay algunos registros con AgeBand 0, lo que corresponde a una edad de entre
# 0 y 5 años, se elimian por entender que son un error al momento de la 
# obtención de los datos.
df <- df[AgeBand != 0]

# Se eliminan los campeonates mundiales, ya que no en estos los atletas 
# ya están clasificados. Para esto se filtran los registros que tengan EventLocation
# distinto a "IRONMAN 70.3 World Championship". Se guardan para agregarse al final,
# para tener todos los datos.

campeonatos <- df[EventLocation == "IRONMAN 70.3 World Championship"]
df <- df[EventLocation != "IRONMAN 70.3 World Championship"]

# Hay combinaciones de EventLocation y EventYear que tiene menos de
# 40 participanes, se eliminan por entender que tienen datos faltantes.

# Cantidad de participantes
cantidad_participantes <- df[, .N, "EventLocation,EventYear"]

df <- cantidad_participantes[df, on = c("EventLocation", "EventYear")]

df <- df[N > 40]

# Ya no es de utilidad la columna cantidad de participantes
df$N <- NULL

# Cálculo de cupos ----

# Proporción género por evento ----
proporcion_genero <- df[, .N, "Gender,EventLocation,EventYear"]

proporcion_genero[, N_total_genero := sum(N), "EventLocation,EventYear"]

proporcion_genero[, prop_genero := N / N_total_genero]

proporcion_genero[, c("N", "N_total_genero") := NULL]


# Proporción por grupo de edad dentro de cada género por evento ----
proporcion_edad <- df[, .N, "Gender,AgeBand,EventLocation,EventYear"]

proporcion_edad[, N_total_genero_edad := sum(N), "Gender,EventLocation,EventYear"]

proporcion_edad[, prop_genero_edad := N / N_total_genero_edad]

proporcion_edad[, c("N", "N_total_genero_edad") := NULL]

# Join
proporcion <- proporcion_genero[proporcion_edad, on = c("Gender","EventLocation", "EventYear")]
  

# Repartiendo cupos ----
eventos <- unique(df[, EventLocation, EventYear]) 

cupos <- copy(proporcion)[Gender == ""] #Copia vacia de proporcion donde se agregarán los cupos

cupos[, c("prop_genero", "prop_genero_edad") := NULL]

cupos[, cupos := 0]

for (fila in seq(1, nrow(eventos))){
  # Extracción de un evento en partícular
  evento <- eventos[fila]
  aux <- proporcion[EventLocation == evento$EventLocation & EventYear == evento$EventYear]
  
  if (grepl("CHAMPIONSHIP", aux$EventLocation[1], ignore.case = TRUE) == TRUE){
    # A los campeonatos les correspoden 80 cupos
    cupos_a_repartir <- 80
  } else{
    # A los eventos "normales" les correspodneo 40 cupos
    cupos_a_repartir <- 40
  }
  
  # Paso 1: 1 cupo por Gender-AgeBand 
  aux[, cupos := 1]
  
  cupos_restantes <- cupos_a_repartir - nrow(aux) 
  
  # Paso 2: Se divide por género
  prop_generos <- unique(aux[, .(Gender, prop_genero)])
  
  prop_generos[, cupos_adicionales := round(prop_genero * cupos_restantes)]
  
  # Paso 3: Se divide por peso de AgeBand dentro de cada género
  prop_generos_edades <- prop_generos[aux, on = c("Gender")]
  
  # Cupos adicionales
  prop_generos_edades[, cupos_adicionales := round(cupos_adicionales * prop_genero_edad)]
  
  # Al redondear puede pasar que no se repartan todos los cupos, por lo que, en caso de
  # sobrar cupos, se reparten siguiendo el orden de los decimales de la proporción.
  cupos_repartidos <- prop_generos_edades[, sum(cupos_adicionales)]
  cupos_restantes <- cupos_restantes - cupos_repartidos
  
  if (cupos_restantes > 0){
    # Ordenamos prop_generos_edades según los decimales de prop_genero_edad de forma descendente
    setorder(prop_generos_edades, -prop_genero_edad)
    
    # Controlamos si se repartido todo
    while (cupos_restantes != 0){
      # Recorremos el datatable prop_genero_edades y vamos dando 1 cupo en orden
      fila <- 1
      while(fila <= nrow(prop_generos_edades) & cupos_restantes != 0){
        prop_generos_edades[fila, cupos_adicionales := cupos_adicionales + 1 ]
        cupos_restantes <- cupos_restantes - 1
        fila <- fila + 1
      }
    }
  }
  
  # Se suman cupos y cupos adicionales
  prop_generos_edades[, cupos := cupos + cupos_adicionales]
  
  # Se seleccionan columnas de interes
  prop_generos_edades <- prop_generos_edades[, .(Gender, AgeBand, EventLocation,
                                                 EventYear, cupos)]
  
  # Los cupos se van guardando en el datatable "cupos"
  cupos <- rbind(cupos, prop_generos_edades)
}

# Los cupos son para cada combinación Gender - Ageband, por lo que para simplificar
# la parte siguiente se crea una nueva variable "categoria" usando estás dos.
cupos[, categoria := paste(Gender, AgeBand, sep = "-")]

cupos[, c("Gender", "AgeBand") := NULL]

# Asignación de cupos ----
# Agregamos la columna "categoria" a df
df[, categoria := paste(Gender, AgeBand, sep = "-")]

# Se creara una columna "clasifica" que tendrá un 1 si el participante
# clasifica y un 0 si no.

# Al principio nadie clasifica.
df[, clasifica := 0]

# Se ordena df según FinishTime
setorder(df, FinishTime)

for (fila in seq(1, nrow(eventos))){
  # Extracción de un evento en partícular
  evento <- eventos[fila]
  
  # Cupos para ese evento
  cupos_evento <- cupos[EventLocation == evento$EventLocation & EventYear == evento$EventYear]
  
  # Categorias para ese evento
  categorias <- unique(cupos_evento$categoria)
  
  print(fila)
  
  # Se recorren las categorias de ese evento
  for (cat in categorias){
    #cupos_cat tiene la cantidad de cupos
    cupos_cat <- cupos_evento[categoria == cat, cupos]
    
    # A los primos "cupos_cat" se le asigna un 1, que quiere decir que clasifica. 
    df[EventLocation == evento$EventLocation & EventYear == evento$EventYear & categoria == cat][1:cupos_cat]$clasifica <- 1
  }
}


# Se agrega la categoria a los campeonatos mundiales
campeonatos[, categoria := paste(Gender, AgeBand, sep = "-")]

# Se agregan los campeonatos mundiales, con un nan en el campo clasifica
campeonatos[, clasifica := NA_integer_]


df <- rbind(df, campeonatos)


fwrite(df, "df_con_cupos.csv")

