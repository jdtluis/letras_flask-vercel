from flask import Flask, jsonify, request
import pandas as pd
from utils import letras as lt
import sqlite3


app = Flask(__name__)


@app.get("/letras")
def get_ledes():
    # http://127.0.0.1:5000/letras?tipo=lecer&date=2023-03-23
    letra = request.args.get('tipo', default='ledes', type=str)
    date = request.args.get('date', default='2023-03-27', type=str)
    data = getdata(letra, date)
    if data.empty:
        d = {}
    else:
        data['TIR'] = round(data['TIR'], 4)
        dm, fit = lt.fitCurve(data['DM'], data['TIR'])
        #fitted = [list(d) for d in zip(dm.round(2).tolist(), fit.round(2).tolist())]
        d = {
            'DM' : data['DM'].to_list(),
            'TIR': data['TIR'].to_list(),
            'DMF': dm.round(2).tolist(),
            'FIT': fit.round(2).tolist()}
    return jsonify(d)


def getdata(letra, date):
    con = sqlite3.connect('data/letras.db')
    data = con.execute(
        f'SELECT Especie, FechaPrecio, CAST(DM as int) DM, TIR FROM {letra} WHERE FechaPrecio = "{date}" ORDER BY '
        f'cast(DM as int) asc').fetchall()
    con.close()
    if not data:
        return pd.DataFrame([])
    else:
        data = pd.DataFrame(data, columns=['Especie', 'FechaPrecio', 'DM', 'TIR'])
        data = data[~data['DM'].isna()]
        return data


#if __name__ == "__main__":
    #app.run(debug = True, passthrough_errors=True) #, host='0.0.0.0', port=8080
