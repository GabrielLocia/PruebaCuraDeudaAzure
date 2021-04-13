from flask import Flask, jsonify, request
import pandas as pd
import pymysql.cursors



app = Flask(__name__)


Codigos = []
Estados = []
StrCodigos = []
filtrado = []


@app.route('/')
def home():
    return 'hello wold'


@app.route('/Carga')
def cargar():

    datos = pd.read_csv('./files/Guerrero.csv', header=0)
    # Obteniendo Códigos postales de archivos SEPOMEX.
    chek = datos['d_codigo'].isnull()

    Codigos = datos['d_codigo'].tolist()
    for i, c in zip(Codigos, chek):
        if (c != True):
            enteros = int(i)
            strc = str(enteros)
            StrCodigos.append(strc)
    # obteniendo Nombre de estado y codigo
    filtrado = datos['d_estado'].dropna()

    Estados = filtrado.tolist()

    filtrado = pd.DataFrame(None)

    filtrado = datos['c_estado'].dropna()
    codigoEstado = filtrado.tolist()

    # Obteniendo nombre de municipios
    filtrado = pd.DataFrame(None)
    filtrado = datos['D_mnpio'].dropna()
    municipios = filtrado.tolist()

    # obteniendo nombre e id de la colonia
    filtrado = pd.DataFrame(None)
    filtrado = datos['d_asenta'].dropna()
    colonias = filtrado.tolist()

    filtrado = pd.DataFrame(None)
    filtrado = datos['id_asenta_cpcons'].dropna()
    idCol = filtrado.tolist()

    # Ciclo para guardar los registros en la base de datos, con posibles mejoras para carga de datos de gran volumen
    for es, ces, nomM, col, idcol, cp in zip(Estados, codigoEstado, municipios, colonias, idCol, StrCodigos):

        connection = pymysql.connect(user='root',
                                     password='',
                                     database='codigospostales_bd',
                                     host='localhost',
                                     cursorclass=pymysql.cursors.DictCursor,
                                     ssl={'ca': './BaltimoreCyberTrustRoot.crt.pem'}
                                     )

        with connection:
            with connection.cursor(pymysql.cursors.SSCursor) as cursor:

                # Insertar Estados
                cursor.execute(
                    'INSERT INTO estados(idEstado,Nombre,CodigoEstado) VALUES(%s,%s,%s)', (0, es, ces))
                connection.commit()

                # Obteniendo el id del ultimo estado registrado
                cursor.execute('SELECT MAX(idEstado) AS idEstado FROM estados')
                resultid = cursor.fetchone()

                # Obteniendo el nombre del estado
                cursor.execute(
                    'SELECT estados.Nombre FROM estados WHERE estados.idEstado=%s', (resultid))
                nomEstado = cursor.fetchone()

                # Obteniendo el codigo de estado
                cursor.execute(
                    'SELECT CodigoEstado FROM estados WHERE estados.idEstado=%s', (resultid))
                result = cursor.fetchone()
                print(result)

                # Insertando Municipio
                cursor.execute(
                    'INSERT INTO municipios(idMunicipio,Nombre,idEs) VALUES(%s,%s,%s)', (0, nomM, result))
                connection.commit()

                # Obteniendo último id ingresado en municipios
                cursor.execute(
                    'SELECT MAX(idMunicipio) AS idMunicipio FROM municipios')
                resultidMunicipios = cursor.fetchone()

                # Obteniendo nombre del municipio
                cursor.execute(
                    'SELECT Nombre FROM municipios WHERE municipios.idMunicipio=%s', (resultidMunicipios))
                nomMunicipio = cursor.fetchone()

                # Insertando Colonias
                cursor.execute(
                    'INSERT INTO colonias(idColonias,ColoniasId,Nombre, IdMunicipio) VALUES(%s,%s,%s,%s)', (0, idcol, col, nomMunicipio))
                connection.commit()

                # Obteniendo el último id de colonias
                cursor.execute(
                    'SELECT MAX(idColonias) AS idColonias FROM colonias')
                resultidColonias = cursor.fetchone()

                """   Obteniendo  id de las colonias 
                cursor.execute(
                    'SELECT idColonias FROM colonias WHERE idColonias=%s', (resultidColonias))
                idcolonias = cursor.fetchone()"""

                # Insertando Codigos Postales
                cursor.execute(
                    'INSERT INTO codigos_postales(idCodigosP,CP,Estado,idColonias) VALUES(%s,%s,%s,%s)', (0, cp, nomEstado, resultidColonias))
                connection.commit()

            connection.commit()
    StrCodigos.clear()

    return jsonify(StrCodigos)

# Ruta para obtener los registros de las colonias mediante el código postal


@app.route('/colonias/<int:CP>')
def colonia(CP):
    mostrar = []
    if type(CP) == int:
        connection = pymysql.connect(user='root',
                                     password='',
                                     database='codigospostales_bd',
                                     host='localhost',
                                     cursorclass=pymysql.cursors.DictCursor,
                                     ssl={'ca': './BaltimoreCyberTrustRoot.crt.pem'}
                                     )
        CP = str(CP)
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    'SELECT * FROM `colonias` INNER JOIN codigos_postales on colonias.idColonias=codigos_postales.idColonias where codigos_postales.CP=%s', (CP))
                colonias = cursor.fetchall()
        i = 0
        for col in colonias:
            print('COL: ', col)
            cp = col['CP']
            co = col['Nombre']
            mu = col['IdMunicipio']
            es = col['Estado']

            print('CP: ', cp)
            print('co: ', co)
            print('mu: ', mu)
            print('es: ', es)
            aux = [
                {
                    "CP": cp,
                    "Colonia": co,
                    "Municipio": mu,
                    "Estado": es
                }
            ]

            mostrar.append(aux)

            print('Mostrar: ', mostrar)
            i = i + 1

        return jsonify(mostrar)
    return jsonify([{'Error': 'Debe ingresar un Número Entero'}])

# Ruta para obtener los registros de las colonias mediante el nombre


@app.route('/coloniasNombre/<nombre>')
def coloniaNombre(nombre):
    mostrar = []
    connection = pymysql.connect(user='root',
                                 password='',
                                 database='codigospostales_bd',
                                 host='localhost',
                                 cursorclass=pymysql.cursors.DictCursor,
                                 ssl={'ca': './BaltimoreCyberTrustRoot.crt.pem'}
                                 )

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT * FROM `colonias` INNER JOIN codigos_postales on colonias.idColonias=codigos_postales.idColonias where colonias.Nombre=%s', (nombre))
            colonias = cursor.fetchall()
        i = 0
        for col in colonias:
            print('COL: ', col)
            cp = col['CP']
            co = col['Nombre']
            mu = col['IdMunicipio']
            es = col['Estado']

            print('CP: ', cp)
            print('co: ', co)
            print('mu: ', mu)
            print('es: ', es)
            aux = [
                {
                    "CP": cp,
                    "Colonia": co,
                    "Municipio": mu,
                    "Estado": es
                }
            ]

            mostrar.append(aux)

            print('Mostrar: ', mostrar)
            i = i + 1

        return jsonify(mostrar)

# Ruta para obtener los registros de los municipios mediante el nombre


@app.route('/municipiosNombre/<nombre>')
def municipioNombre(nombre):
    mostrar = []
    connection = pymysql.connect(user='root',
                                 password='',
                                 database='codigospostales_bd',
                                 host='localhost',
                                 cursorclass=pymysql.cursors.DictCursor,
                                 ssl={'ca': './BaltimoreCyberTrustRoot.crt.pem'}
                                 )

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT * FROM colonias INNER JOIN codigos_postales on colonias.idColonias=codigos_postales.idColonias where colonias.IdMunicipio=%s', (nombre))
            colonias = cursor.fetchall()
        i = 0
        for col in colonias:
            print('COL: ', col)
            cp = col['CP']
            co = col['Nombre']
            mu = col['IdMunicipio']
            es = col['Estado']

            print('CP: ', cp)
            print('co: ', co)
            print('mu: ', mu)
            print('es: ', es)
            aux = [
                {
                    "CP": cp,
                    "Colonia": co,
                    "Municipio": mu,
                    "Estado": es
                }
            ]

            mostrar.append(aux)

            print('Mostrar: ', mostrar)
            i = i + 1

        return jsonify(mostrar)

# Ruta para obtener los registros de los estados mediante el nombre


@app.route('/estadoNombre/<nombre>')
def estadoNombre(nombre):
    mostrar = []
    connection = pymysql.connect(user='root',
                                 password='',
                                 database='codigospostales_bd',
                                 host='localhost',
                                 cursorclass=pymysql.cursors.DictCursor,
                                 ssl={'ca': './BaltimoreCyberTrustRoot.crt.pem'}
                                 )

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT * FROM `colonias` INNER JOIN codigos_postales on colonias.idColonias=codigos_postales.idColonias where codigos_postales.Estado=%s', (nombre))
            colonias = cursor.fetchall()
        i = 0
        for col in colonias:
            print('COL: ', col)
            cp = col['CP']
            co = col['Nombre']
            mu = col['IdMunicipio']
            es = col['Estado']

            print('CP: ', cp)
            print('co: ', co)
            print('mu: ', mu)
            print('es: ', es)
            aux = [
                {
                    "CP": cp,
                    "Colonia": co,
                    "Municipio": mu,
                    "Estado": es
                }
            ]

            mostrar.append(aux)

            print('Mostrar: ', mostrar)
            i = i + 1

    return jsonify(mostrar)


@app.route('/agregar', methods=['POST'])
def agregar():
    print(request.json)
    connection = pymysql.connect(user='root',
                                 password='',
                                 database='codigospostales_bd',
                                 host='localhost',
                                 cursorclass=pymysql.cursors.DictCursor,
                                 ssl={'ca': './BaltimoreCyberTrustRoot.crt.pem'}
                                 )

    with connection:
        with connection.cursor(pymysql.cursors.SSCursor) as cursor:

            # Insertar Estados
            cursor.execute(
                'INSERT INTO estados(idEstado,Nombre,CodigoEstado) VALUES(%s,%s,%s)', (0, request.json['Estado'], request.json['idEstado']))
            connection.commit()

            # Obteniendo el id del ultimo estado registrado
            cursor.execute('SELECT MAX(idEstado) AS idEstado FROM estados')
            resultid = cursor.fetchone()

            # Obteniendo el nombre del estado
            cursor.execute(
                'SELECT estados.Nombre FROM estados WHERE estados.idEstado=%s', (resultid))
            nomEstado = cursor.fetchone()

            # Obteniendo el codigo de estado
            cursor.execute(
                'SELECT CodigoEstado FROM estados WHERE estados.idEstado=%s', (resultid))
            result = cursor.fetchone()
            print(result)

            # Insertando Municipio
            cursor.execute(
                'INSERT INTO municipios(idMunicipio,Nombre,idEs) VALUES(%s,%s,%s)', (0, request.json['Municipio'], result))
            connection.commit()

            # Obteniendo último id ingresado en municipios
            cursor.execute(
                'SELECT MAX(idMunicipio) AS idMunicipio FROM municipios')
            resultidMunicipios = cursor.fetchone()

            # Obteniendo nombre del municipio
            cursor.execute(
                'SELECT Nombre FROM municipios WHERE municipios.idMunicipio=%s', (resultidMunicipios))
            nomMunicipio = cursor.fetchone()

            # Insertando Colonias
            cursor.execute(
                'INSERT INTO colonias(idColonias,ColoniasId,Nombre, IdMunicipio) VALUES(%s,%s,%s,%s)', (0, request.json['idColonia'], request.json['Colonia'], nomMunicipio))
            connection.commit()

            # Obteniendo el último id de colonias
            cursor.execute(
                'SELECT MAX(idColonias) AS idColonias FROM colonias')
            resultidColonias = cursor.fetchone()

            """   Obteniendo  id de las colonias 
                cursor.execute(
                    'SELECT idColonias FROM colonias WHERE idColonias=%s', (resultidColonias))
                idcolonias = cursor.fetchone()"""

            # Insertando Codigos Postales
            cursor.execute(
                'INSERT INTO codigos_postales(idCodigosP,CP,Estado,idColonias) VALUES(%s,%s,%s,%s)', (0, request.json['CP'], request.json['Estado'], resultidColonias))
            connection.commit()

        connection.commit()

    return jsonify([{'Mensaje': 'Registro Guardado con éxito'}])


if __name__ == '__main__':
    app.run(debug=True)
