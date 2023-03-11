import pymysql
import pandas as pd
# Conectar a la base de datos MySQL
connection = pymysql.connect(
    host = "localhost",
    user = "newuser",
    password = "luis023",
    db = ""
)

cursor = connection.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS indicadores_mundiales")
cursor.execute("USE indicadores_mundiales")

# Leer los datos del archivo CSV con pandas
datos = pd.read_csv('./data_normalizada.csv', sep='\t')
datos.fillna(0, inplace=True)

# Crear la tabla si no existe
cursor.execute("""
    CREATE TABLE IF NOT EXISTS indicadores_mundiales (
        id INT PRIMARY KEY AUTO_INCREMENT,
        country VARCHAR(255) NOT NULL,
        year INT NOT NULL,
        pib_percapita_usd FLOAT NOT NULL,
        inflacion_porcentual FLOAT NOT NULL,
        Tasa_de_Interes FLOAT NOT NULL,
        tasa_cambio_USD FLOAT NOT NULL,
        saldo_cuenta_corriente FLOAT NOT NULL,
        deuda_externa_porcentaje_inb FLOAT NOT NULL,
        inversion_extranjera_directa_entrada_perct FLOAT NOT NULL,
        desempleo_total_pct FLOAT NOT NULL
    )
""")

# Insertar los datos en la tabla si aún no existen
for index, row in datos.iterrows():
    country = row['country']
    year = row['year']
    pib_percapita_usd = row['PIB_percapita_USD']
    inflacion_porcentual = row['inflacion_porcentual']
    tasa_de_interes = row['Tasa de Interes']
    tasa_cambio_usd = row['tasa_cambio_USD']
    saldo_cuenta_corriente = row['saldo_cuenta_corriente']
    deuda_externa_porcentaje_inb = row['deuda_externa_porcentaje_inb']
    inversion_extranjera_directa_entrada_perct = row['inversion_extranjera_directa_entrada_perct']
    desempleo_total_pct = row['desempleo_total_pct']
    cursor.execute("SELECT id FROM indicadores_mundiales WHERE country = %s AND year = %s", (country, year))
    resultado = cursor.fetchone()
    if resultado is None:
        cursor.execute("INSERT INTO indicadores_mundiales (country, year, pib_percapita_usd, inflacion_porcentual, Tasa_de_Interes, tasa_cambio_USD, saldo_cuenta_corriente, deuda_externa_porcentaje_inb, inversion_extranjera_directa_entrada_perct, desempleo_total_pct) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (country, year, pib_percapita_usd, inflacion_porcentual, tasa_de_interes, tasa_cambio_usd, saldo_cuenta_corriente, deuda_externa_porcentaje_inb, inversion_extranjera_directa_entrada_perct, desempleo_total_pct))
        connection.commit()

# Cerrar la conexión a la base de datos
cursor.close()
connection.close()
