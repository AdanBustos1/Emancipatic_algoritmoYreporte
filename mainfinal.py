from ast import Try

import funcionesfinal as fnfinal

from fastapi import FastAPI
app = FastAPI()


@app.get("/Api")
def hello():
  return {"Esta funcionando la Api!"}



#http://127.0.0.1:8000/Recomendados?idUsuario=62e562fe032e876acb908167
@app.get("/Recomendados")
def obtenerDatosRecomendados(idUsuario:str): 
  return fnfinal.obtenerRecomendados(idUsuario,3)
