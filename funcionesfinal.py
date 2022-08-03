import numpy as np
import pandas as pd 
import pymongo
from bson import ObjectId
from ast import literal_eval


#Funcion para conectar base de datos

def CargarDataframe(nombreColeccion):
    URI_CONNECTION="mongodb+srv://grupo1tripulaciones:grupo1tripulaciones@cluster0.cm9sh0x.mongodb.net/emancipatic?retryWrites=true&w=majority"
    NOMBRE_BBDD = 'emancipatic'
    cliente = pymongo.MongoClient(URI_CONNECTION)
    baseDatos = cliente[NOMBRE_BBDD]
    coleccion = baseDatos[nombreColeccion]
    dataframe = pd.DataFrame(list(coleccion.find()))
    cliente.close()
    return dataframe



def obtenerValoracionesByIdUsuario(id_usuario):
    URI_CONNECTION="mongodb+srv://grupo1tripulaciones:grupo1tripulaciones@cluster0.cm9sh0x.mongodb.net/emancipatic?retryWrites=true&w=majority"
    NOMBRE_BBDD = 'emancipatic'
    cliente = pymongo.MongoClient(URI_CONNECTION)
    baseDatos = cliente[NOMBRE_BBDD]
    coleccion = baseDatos['valoraciones_juego']
    dataframe = pd.DataFrame(list(coleccion.find({"id_usuario": ObjectId(id_usuario)}) ))
    cliente.close()
    return dataframe


def obtenerListaJuegos(df_id_juego):
    URI_CONNECTION="mongodb+srv://grupo1tripulaciones:grupo1tripulaciones@cluster0.cm9sh0x.mongodb.net/emancipatic?retryWrites=true&w=majority"
    NOMBRE_BBDD = 'emancipatic'
    cliente = pymongo.MongoClient(URI_CONNECTION)
    baseDatos = cliente[NOMBRE_BBDD]
    coleccion = baseDatos['juegos']
    lista = []
    for i in df_id_juego.index:
        id = df_id_juego['id_juego'][i]
        resultado = coleccion.find({"_id": ObjectId(id)})
        for elemento in resultado:
            elemento['_id']=str(elemento['_id'])
            lista.append(elemento) 
    cliente.close()
    return lista


def obtenerRecomendados(id_usuario,num_recomendados):

    #Obtener el listado de juegos,
    df_juegos= CargarDataframe('juegos')

    #Manipular el campo que contiene el listado de ObjectsId decategorias del juego, para que evalue cada id de categoria como una exprexion completa, y no como caracteres sueltos
    df_juegos['categorias'] = df_juegos['categorias'].apply(lambda x: x.replace("ObjectId(",""))
    df_juegos['categorias'] = df_juegos['categorias'].apply(lambda x: x.replace(")",""))
    df_juegos['categorias'] = df_juegos['categorias'].apply(literal_eval)


    #Convertir el ObjectId de juego para que sea de tipo string y permitirá hacer querys en el dataframe
    list_id = []
    for i in df_juegos.index:
        nuevo_id = str(ObjectId(df_juegos['_id'][i]))  
        list_id.append(nuevo_id)
    df_juegos['nuevo_id'] = list_id

    #limpieza de columnas, y reordenacion de las mismas para dejar el dataframe limpio
    df_juegos= df_juegos.drop(['_id','nivel_juego', 'descripcion_juego','imagen_juego', 'instrucciones_juego', 'createdAt', 'updatedAt'], axis=1)
    df_juegos.rename(columns= {'nuevo_id':'_id'},inplace=True)
    df_juegos= df_juegos.reindex(columns=["_id", 'titulo_juego', 'categorias', 'promedio_valoracion'])


    df_juegos2= df_juegos.copy()

    #Transformar el dataframe de juegos, para que las categorias se pongan como columnas
    for index, row in df_juegos2.iterrows():
        for categ in row['categorias']: 
            df_juegos2.at[index,categ]= 1

    df_juegos2 = df_juegos2.fillna(0) #rellenar con 0 los campos vacios


    df = obtenerValoracionesByIdUsuario(id_usuario) #obtener datos para un usuario determinado


    #Convertir los campos objectId a strig para poder interactuar con ellos en los dataframe
    list_id_juego = []
    list_id_usuario = []
    for i in df.index:
        nuevo_id_juego = str(ObjectId(df['id_juego'][i]))  #este campo ya será de tipo string y permitirá hacer querys en el dataframe
        list_id_juego.append(nuevo_id_juego)
        nuevo_id_usuario = str(ObjectId(df['id_usuario'][i]))  #este campo ya será de tipo string y permitirá hacer querys en el dataframe
        list_id_usuario.append(nuevo_id_usuario)
    df['nuevo_id_juego'] = list_id_juego
    df['nuevo_id_usuario'] = list_id_usuario

    df_valoracion_usuario = df.drop(['_id','id_usuario','id_juego','createdAt','updateAt'], axis=1) #eliminar columnas que no hacen falta
    df_valoracion_usuario.rename(columns= {'nuevo_id_juego':'id_juego','nuevo_id_usuario':'id_usuario'},inplace=True) #renombrar columnas 


    #obtener un dataframe de los juegos valorados por el usuario pero del datafreame general de juegos
    df_juegos.rename(columns= {'_id':'id_juego'},inplace=True) 
    df_datos_juegos = df_juegos[df_juegos["id_juego"].isin(df_valoracion_usuario["id_juego"].tolist())] #sacar las pelis del usuario en la tabla original


    #al dataframe de valoracion se le añaden los datos del juego
    df_datos_juegos = df_datos_juegos.drop(["promedio_valoracion"], axis=1)
    df_valoracion_usuario = pd.merge(df_datos_juegos, df_valoracion_usuario)

    #conseguir el dataframe de los juegos valorados por el usuario pero  con los datos de las categorias
    df_valoracion_usuario = df_valoracion_usuario.drop(["categorias"],axis=1)
    df_juegos_usuario = df_juegos2[df_juegos2["_id"].isin(df_valoracion_usuario["id_juego"].tolist())]

    #para generar un tabla de categorias 
    df_juegos_usuario = df_juegos_usuario.reset_index(drop=True)
    df_tabla_categorias = df_juegos_usuario.drop(["_id", "titulo_juego", "categorias"], axis=1)


    #comienzo del algoritmo
    perfil_usuario = df_tabla_categorias.transpose()
    perfil_usuario = df_tabla_categorias.transpose().dot(df_valoracion_usuario["valoracion"])
    categorias = df_juegos2.set_index(df_juegos2["_id"])
    categorias = categorias.drop(["_id", "titulo_juego", "categorias"],axis=1)
    recomendados = ((categorias*perfil_usuario).sum(axis=1))/(perfil_usuario.sum())
    recomendados = recomendados.sort_values(ascending=False)

    final_juegos = df_juegos.loc[df_juegos["id_juego"].isin(recomendados.head(num_recomendados).keys())]

    lista = obtenerListaJuegos(final_juegos)

    return lista


