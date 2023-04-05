import numpy as np
import pandas as pd
import requests
import io
import datetime as dt
import tabula
from bs4 import BeautifulSoup
from time import sleep
import sqlite3
from scipy.interpolate import CubicSpline
from scipy.optimize import least_squares


mapping_dict = {
    'JAN': 'JAN',
    'ENE': 'JAN',
    'FEB': 'FEB',
    'MAR': 'MAR',
    'ABR': 'APR',
    'APR': 'APR',
    'MAY': 'MAY',
    'JUN': 'JUN',
    'JUL': 'JUL',
    'AGO': 'AUG',
    'SEP': 'SEP',
    'OCT': 'OCT',
    'NOV': 'NOV',
    'DIC': 'DEC'}


def getLinks():
    iamcInf = requests.get("https://www.iamc.com.ar/informeslecap/")
    print(iamcInf.status_code)
    soup = BeautifulSoup(iamcInf.content, "html.parser")
    content_links = soup.findAll(attrs="contenidoListado Acceso-Rapido")
    links = []
    for c in content_links:
        attrs = c.find('a')
        links.append(attrs.attrs.get('href'))
    return links


def transformData(letras, isledes=True):
    letras = letras.astype('str')
    letras = letras[~letras.iloc[:, 0].isna()]
    if isledes:
        letras = letras[letras.iloc[:, 0].str[0] == 'S']
    else:
        letras = letras[letras.iloc[:, 0].str[0] == 'X']
    letras = letras.replace('na', np.nan, regex=True)
    letras = letras.drop(columns=letras.columns[letras.isna().all()].values.tolist())
    list_splited = []
    for i in (0, 1):
        if letras.iloc[:, i].str.split(' ', expand=True).shape[1] > 1:
            splited = letras.iloc[:, i].str.split(' ', expand=True).iloc[:, [0, 2]]
        else:
            splited = letras.iloc[:, i]
        list_splited.append(splited)
    all_splited = pd.concat(list_splited, axis=1)
    letras = pd.merge(all_splited, letras.iloc[:, 2:], left_index=True, right_index=True)

    columns = ['Especie', "Emision", "Pago", "Plazo", "Monto", "FechaPrecio", "Precio", "Rendimiento", "TNA", "TIR", "DM", "PF"]
    if not isledes:
        columns.pop(columns.index("PF"))
        columns.insert(3, 'CERinicial')
    letras.columns = columns
    letras[["Rendimiento", "TNA", "TIR"]] = letras[["Rendimiento", "TNA", "TIR"]].replace('%', '', regex=True).replace('[a-z]', np.nan, regex=True)
    letras["DM"] = letras["DM"].replace('-', np.nan, regex=True)
    # Fields to float
    if not isledes:
        letras['CERinicial'] = letras['CERinicial'].astype('float')
    letras['Precio'] = letras['Precio'].astype('float')
    letras[["Rendimiento", "TNA", "TIR"]] = letras[["Rendimiento", "TNA", "TIR"]].astype('float') / 100
    # Fields to date
    for i in ["Emision", 'Pago', 'FechaPrecio']:
        letras[i] = letras[i].apply(lambda x: x.replace(x.split('-')[1], x.split('-')[1].upper()))
        letras[i] = letras[i].apply(
            lambda x: dt.datetime.strptime(x.replace(x.split('-')[1], mapping_dict[x.split('-')[1]].capitalize()),
                                           "%d-%b-%y").date())
    return letras


def getData(links):
    ledesAll = []
    lecerAll = []
    for l in links:
        r = requests.get(l)
        f = io.BytesIO(r.content)
        ledesAll.append(transformData(tabula.read_pdf(f, area=(130, 3, 220, 550), pages=1, multiple_tables=True)[0], True))
        lecerAll.append(transformData(tabula.read_pdf(f, area=(290, 3, 370, 550), pages=1, multiple_tables=True)[0], False))
        if len(ledesAll) != len(lecerAll):
            print(l)
            break
        sleep(5)
    return ledesAll, lecerAll


def insert(letras, name='ledes'):
    dates = getdates(name)
    new_dates = letras['FechaPrecio'][~letras['FechaPrecio'].astype('str').isin(dates)].unique()
    to_insert = letras[letras['FechaPrecio'].isin(new_dates)]
    to_insert.to_sql(name=name, con=con, if_exists='append')


def getdates(letras='ledes'):
    cur = con.execute(f"SELECT FechaPrecio FROM {letras} GROUP BY FechaPrecio")
    dates = pd.DataFrame(cur.fetchall())
    d = [i[0] for i in dates.values]
    return d


def fitCurve(DM, TIR):
    cs = CubicSpline(DM, TIR)
    curve_points = np.linspace(DM.min(), DM.max(), 10)
    return curve_points, cs(curve_points)


if __name__ == '__main__':
    con = sqlite3.connect('../data/letras.db')
    links = getLinks()
    dates = pd.DataFrame(getdates(), columns=['Fecha'])
    dateslink = []
    for i, l in enumerate(links):
        temp = l.split('/')[5:7]
        year = temp[0]
        monthday = temp[1].split('_')
        month = monthday[0]
        day = monthday[1]
        if len(monthday[0]) == 1:
            month = '0'+monthday[0]

        if len(monthday[1]) == 1:
            day = '0' + monthday[1]
        dateslink.append((i, year + '-' + month + '-' + day))
    dateslink = pd.DataFrame(dateslink,columns=['id','Fecha'])
    filldates = dateslink.merge(dates, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
    filldatesId = dateslink['id'][dateslink['Fecha'].isin(filldates['Fecha'])].to_list()
    filldateslink = [l for i, l in enumerate(links) if i in filldatesId]

    ledes, lecer = getData(filldateslink)

    #pd.concat(ledes).to_sql(name='ledes', con=con)
    #pd.concat(lecer).to_sql(name='lecer', con=con)
    if len(ledes) > 0 and len(ledes) == len(lecer):
        insert(pd.concat(ledes), 'ledes')
        insert(pd.concat(lecer), 'lecer')
    con.close()
















# import plotly.graph_objects as go
# import plotly.io as pio
# from scipy.interpolate import CubicSpline
# from scipy.optimize import least_squares
# import numpy as np
#
# cs = CubicSpline(ledes['DM'], ledes['TIR'])
# curve_points = np.linspace(ledes['DM'].min(), ledes['DM'].max(), 15)
#
# pio.renderers
# pio.renderers.default = 'browser'
# fig = go.Figure()  #FigureWidget()
# for i, s in enumerate([1,2]):
#     fig.add_scatter(name=f'TIR{s}', line=dict(width=1, dash='dot'),
#                 mode='lines+markers')
#
# fig.data[0].x = ledes['DM']
# fig.data[0].y = ledes['TIR']
# fig.data[1].x = curve_points
# fig.data[1].y = cs(curve_points)  #signal.savgol_filter(ledes['TIR'], 3, 2)
# pio.show(fig)