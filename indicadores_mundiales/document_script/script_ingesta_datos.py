# Importar las bibliotecas necesarias
import requests  # Librería para hacer solicitudes HTTP
import xml.etree.ElementTree as ET # Librería para procesar archivos XML
import pandas as pd # Librería para el análisis de datos
import numpy as np # Librería para cálculos numéricos
import tempfile
import os
import json
from pydrive.auth import GoogleAuth # Librería para autenticación de Google Drive
from pydrive.drive import GoogleDrive # Librería para usar Google Drive




#Saldo en cuenta corriente (% del PIB)

# Obtener el archivo XML
url = "https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/saldo_cuenta_corriente_pib_anual.xml"
response = requests.get(url)

# Leer el contenido del archivo XML 
xml_data = response.content.decode('utf-8')

# Parsear el contenido del archivo XML
root = ET.fromstring(xml_data)

# Crear una lista de diccionarios con los datos de los registros
data = []

for record in root.findall("./data/record"):
    country = record.find("./field[@name='Country or Area']").text
    year = int(record.find("./field[@name='Year']").text)
    value = record.find("./field[@name='Value']").text
    data.append({'country': country, 'year': year, 'value': float(value) if value is not None else np.nan})

# Crear un DataFrame a partir de la lista de diccionarios
df_saldo_cc_pib = pd.DataFrame(data)

# Renombrar la columna 'saldo_cc_pib'
df_saldo_cc_pib = df_saldo_cc_pib.rename(columns={'value': 'saldo_cc_pib_pct'})


#Dataframe Crecimiento_pib
# Obtener datos del XML
url = "https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/crecimiento%20pib%20anual%20(%25).xml"
response = requests.get(url)
xml_data = response.content.decode('utf-8')
root = ET.fromstring(xml_data)

# Extraer datos y guardarlos en una lista
data = []
for record in root.findall("./data/record"):
    country = record.find("./field[@name='Country or Area']").text
    year = int(record.find("./field[@name='Year']").text)
    value = record.find("./field[@name='Value']").text
    if value is not None:
        value = float(value)
    data.append({'country': country, 'year': year, 'crecimiento_pib': value})

# Convertir lista a DataFrame
df_crecimiento_pib = pd.DataFrame(data)

# Llenar valores NaN con np.nan
df_crecimiento_pib['crecimiento_pib'] = df_crecimiento_pib['crecimiento_pib'].fillna(np.nan)

# Renombrar columnas
df_crecimiento_pib = df_crecimiento_pib.rename(columns={'crecimiento_pib': 'crecimiento_pib_porcentual'})



#Datraframe Pib per capital 
# Obtener los datos del XML desde una URL
url = "https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/pib_per_capital.xml"
response = requests.get(url)

# Parsear los datos XML y extraer los elementos necesarios
xml_data = response.content.decode('utf-8')
root = ET.fromstring(xml_data)

data = []
for record in root.findall("./data/record"):
    country = record.find("./field[@name='Country or Area']").text
    year = int(record.find("./field[@name='Year']").text)
    value = record.find("./field[@name='Value']").text
    
    # Agregar valor NaN si no hay valor para el año actual
    if value is None:
        value = float('nan')
    else:
        value = float(value)
    
    data.append({'country': country, 'year': year, 'pib_percapita': value})

# Crear un DataFrame de Pandas a partir de los datos extraídos
df_pib_percapita = pd.DataFrame(data)

# Renombrar columnas
df_pib_percapita = df_pib_percapita.rename(columns={'pib_percapita': 'PIB_percapita_USD'})


#Dataframe inflacion porcentual
url = "https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/inflacion_porcentual.xml"
response = requests.get(url)

# Analizar el contenido XML de la respuesta
root = ET.fromstring(response.content)

# Inicializar una lista para almacenar los datos del archivo XML
data = []

# Iterar sobre los elementos "record" en el archivo XML y extraer los valores de los campos "field"
for record in root.findall('.//record'):
    row = {}
    for field in record.findall('field'):
        row[field.get('name')] = field.text
    data.append(row)

# Convertir la lista de datos en un DataFrame de pandas
df_inflacion = pd.DataFrame(data)

# Limpiar los datos y convertir las columnas apropiadas a un tipo de datos numérico
df_inflacion['Year'] = df_inflacion['Year'].astype(int)
df_inflacion['Value'] = pd.to_numeric(df_inflacion['Value'], errors='coerce')

# Renombrar columnas
df_inflacion = df_inflacion.drop(columns=["Item"])
df_inflacion = df_inflacion.rename({"Country or Area": "country", "Year": "year", "Value": "inflacion_porcentual"}, axis=1)

#Dataframe pago interes

# URL del archivo XML
url = 'https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/pago_interes_porcentual.xml'

# Realizar la solicitud HTTP
response = requests.get(url)

# Analizar el contenido de XML
root = ET.fromstring(response.content)

# Inicializar listas vacías para cada columna
country = []
year = []
interest_payment = []

# Recorrer cada registro del XML
for record in root.findall('.//record'):
    # Obtener los valores de cada campo
    country_or_area = record.find(".//field[@name='Country or Area']")
    item = record.find(".//field[@name='Item']")
    year_value = record.find(".//field[@name='Year']")
    value = record.find(".//field[@name='Value']")
    
    # Agregar los valores a las listas correspondientes
    country.append(country_or_area.text)
    year.append(year_value.text)
    interest_payment.append(value.text)

# Crear el dataframe
df_interest_payment = pd.DataFrame({
    'country': country,
    'year': year,
    'pago_intereses_porcentual': interest_payment
})

# Convertir las columnas de year y pago_intereses a números
df_interest_payment['year'] = pd.to_numeric(df_interest_payment['year'])
df_interest_payment['pago_intereses_porcentual'] = pd.to_numeric(df_interest_payment['pago_intereses_porcentual'])

#Dataframe Tasa de interes
url = "https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/tasa_interes_activa.xml"
response = requests.get(url)

# Decodificar el contenido del archivo XML
xml_data = response.content.decode('utf-8')

# Crear un objeto de ElementTree a partir del archivo XML decodificado
root = ET.fromstring(xml_data)

data = []

# Iterar sobre cada registro en el objeto de ElementTree y extraer la información deseada
for record in root.findall("./data/record"):
    country = record.find("./field[@name='Country or Area']").text
    year = int(record.find("./field[@name='Year']").text)
    value = record.find("./field[@name='Value']").text
    
    # Si el valor es nulo, agregar un NaN al DataFrame
    if value is None:
        data.append({'country': country, 'year': year, 'tasa_interes_porcentual': np.nan})
    # De lo contrario, convertir el valor a float y agregarlo al DataFrame
    else:
        data.append({'country': country, 'year': year, 'tasa_interes_porcentual': float(value)})

# Crear un DataFrame a partir de la lista de diccionarios generada anteriormente
df_tasa_interes = pd.DataFrame(data)

# Renombrar columnas
df_tasa_interes = df_tasa_interes.rename(columns={'tasa_interes_porcentual': 'Tasa de Interes'})

#Dataframe Tasa de cambio
# Leer el archivo XML
url = "https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/tasa_cambio_oficial.xml"
response = requests.get(url)
xml_data = response.content.decode('utf-8')
root = ET.fromstring(xml_data)

# Crear una lista de diccionarios con los datos
data = []
for record in root.findall("./data/record"):
    country = record.find("./field[@name='Country or Area']").text
    year = int(record.find("./field[@name='Year']").text)
    value = record.find("./field[@name='Value']").text
    # Considerar valores NaN
    if value is not None:
        # Convertir a float y agregar a la lista de diccionarios
        data.append({'country': country, 'year': year, 'tasa_cambio_USD': float(value)})
    else:
        # Agregar un diccionario con NaN para el valor faltante
        data.append({'country': country, 'year': year, 'tasa_cambio_USD': float('nan')})

# Convertir la lista de diccionarios en un DataFrame de Pandas
df_tasa_cambio = pd.DataFrame(data)

# Renombrar columnas
df_tasa_cambio = df_tasa_cambio.rename(columns={'tasa_cambio_USD': 'tasa_cambio_USD'})



# Dataframe sado cuenta corriente
# Obtener datos del XML
url = "https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/Saldo%20en%20cuenta%20corriente%20(balanza%20de%20pagos%2C%20US%24%20a%20precios%20actuales).xml"
response = requests.get(url)

xml_data = response.content.decode('utf-8')
root = ET.fromstring(xml_data)

# Crear lista de diccionarios con los datos
data = []
for record in root.findall("./data/record"):
    country = record.find("./field[@name='Country or Area']").text
    year = int(record.find("./field[@name='Year']").text)
    value = record.find("./field[@name='Value']").text
    
    # Agregar valores NaN cuando no hay datos disponibles
    if value is not None:
        value = float(value)
    else:
        value = None
        
    data.append({'country': country, 'year': year, 'saldo_cuenta_corriente_usd': value})

# Crear dataframe con los datos y renombrar columnas
df_saldo_cuenta_corriente = pd.DataFrame(data)
df_saldo_cuenta_corriente = df_saldo_cuenta_corriente.rename(columns={'saldo_cuenta_corriente_usd': 'saldo_cuenta_corriente'})


# Deuda Externa
# Obtener datos del XML
url = "https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/Deuda%20externa%20acumulada%20(%25%20del%20INB).xml"
response = requests.get(url)

xml_data = response.content.decode('utf-8')
root = ET.fromstring(xml_data)

# Crear lista de diccionarios con los datos
data = []
for record in root.findall("./data/record"):
    country = record.find("./field[@name='Country or Area']").text
    year = int(record.find("./field[@name='Year']").text)
    value = record.find("./field[@name='Value']").text
    if value is not None:
        data.append({'country': country, 'year': year, 'deuda_externa': float(value)})
    else:
        data.append({'country': country, 'year': year, 'deuda_externa': np.nan})

# Crear dataframe con los datos y renombrar columnas
df_deuda_externa = pd.DataFrame(data)
df_deuda_externa = df_deuda_externa.rename(columns={'deuda_externa': 'deuda_externa_porcentaje_inb'})




# Dataframe Inversion extranjera directa entrada


url = "https://raw.githubusercontent.com/AngelGarciaU/Proyectos/main/indicadores_mundiales/banco_mundial/client_secrets.json"
response = requests.get(url)
data = json.loads(response.content)

# Guardar el contenido del archivo JSON en un archivo temporal
with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
    json.dump(data, f)
    temp_file_path = f.name

# Cargar los secretos del cliente de Google Drive desde el archivo temporal
gauth = GoogleAuth()
gauth.LoadClientConfigFile(temp_file_path)

# Eliminar el archivo temporal
os.unlink(temp_file_path)

# Utilizar Google Drive aquí
drive = GoogleDrive(gauth)


# Definir el nombre del archivo XML y la carpeta que lo contiene
nombre_archivo = 'Inversión extranjera directa entrada.xml'
folder_id = '1a2n_iMJfnnXPjZBOksoXU3UolAEvj-oT'

# Buscar el archivo por nombre en la carpeta de Google Drive
query = f"'{folder_id}' in parents and trashed=false and title='{nombre_archivo}'"
file_list = drive.ListFile({'q': query}).GetList()

# Descargar el contenido del archivo si se encuentra
if len(file_list) > 0:
    xml_file = file_list[0]
    content = xml_file.GetContentString()

    # Transformar el contenido del archivo XML en un DataFrame de Pandas
    root = ET.fromstring(content)
    data = []
    for record in root.findall("./data/record"):
        country = record.find("./field[@name='Country or Area']").text
        year = int(record.find("./field[@name='Year']").text)
        value = record.find("./field[@name='Value']").text
        
        # Agregar valores NaN cuando no hay datos disponibles
        if value is not None:
            value = float(value)
        else:
            value = None
            
        data.append({'country': country, 'year': year, 'saldo_cuenta_corriente_usd': value})
    
    # Crear DataFrame y renombrar columnas
    df = pd.DataFrame(data)
    df_inversion_extranjera_directa_entrada = df.rename(columns={'saldo_cuenta_corriente_usd': 'inversion_extranjera_directa_entrada_perct'})
    
    # Mostrar DataFrame resultante
    #print(df_inversion_extranjera_directa_entrada)
else:
    print(f"No se encontró el archivo '{nombre_archivo}' en la carpeta.")

# Dataframe Inversion extranjera directa salida
nombre_archivo = 'inversion extranjera neta salida.xml'
folder_id = '1a2n_iMJfnnXPjZBOksoXU3UolAEvj-oT'

# Buscar el archivo por nombre en la carpeta de Google Drive
query = f"'{folder_id}' in parents and trashed=false and title='{nombre_archivo}'"
file_list = drive.ListFile({'q': query}).GetList()

# Descargar el contenido del archivo si se encuentra
if len(file_list) > 0:
    xml_file = file_list[0]
    content = xml_file.GetContentString()

    # Transformar el contenido del archivo XML en un DataFrame de Pandas
    root = ET.fromstring(content)
    data = []
    for record in root.findall("./data/record"):
        country = record.find("./field[@name='Country or Area']").text
        year = int(record.find("./field[@name='Year']").text)
        value = record.find("./field[@name='Value']").text
        
        # Agregar valores NaN cuando no hay datos disponibles
        if value is not None:
            value = float(value)
        else:
            value = None
            
        data.append({'country': country, 'year': year, 'saldo_cuenta_corriente_usd': value})
    
    # Crear DataFrame y renombrar columnas
    df = pd.DataFrame(data)
    df_inversion_extranjera_directa_salida = df.rename(columns={'saldo_cuenta_corriente_usd': 'inversion_extranjera_directa_salida_perct'})
    
    # Mostrar DataFrame resultante
    #print(df_inversion_extranjera_directa_salida)
else:
    print(f"No se encontró el archivo '{nombre_archivo}' en la carpeta.")

# Dataframe Índices Global Equity
nombre_archivo = 'Índices Global Equity de S&P.xml'
folder_id = '1a2n_iMJfnnXPjZBOksoXU3UolAEvj-oT'

# Buscar el archivo por nombre en la carpeta de Google Drive
query = f"'{folder_id}' in parents and trashed=false and title='{nombre_archivo}'"
file_list = drive.ListFile({'q': query}).GetList()

# Descargar el contenido del archivo si se encuentra
if len(file_list) > 0:
    xml_file = file_list[0]
    content = xml_file.GetContentString()

    # Transformar el contenido del archivo XML en un DataFrame de Pandas
    root = ET.fromstring(content)
    data = []
    for record in root.findall("./data/record"):
        country = record.find("./field[@name='Country or Area']").text
        year = int(record.find("./field[@name='Year']").text)
        value = record.find("./field[@name='Value']").text
        
        # Agregar valores NaN cuando no hay datos disponibles
        if value is not None:
            value = float(value)
        else:
            value = None
            
        data.append({'country': country, 'year': year, 'Indice_global_eqity_sp': value})
    
    # Crear DataFrame y renombrar columnas
    df = pd.DataFrame(data)
    df_indice_global_eqity_sp_perct = df.rename(columns={'Indice_global_eqity_sp': 'indice_global_eqity_sp_perct'})
    
    # Mostrar DataFrame resultante
    #print(df_indice_global_eqity_sp_perct)
else:
    print(f"No se encontró el archivo '{nombre_archivo}' en la carpeta.")

# Dataframe desempleo mujeres 
nombre_archivo = 'Desempleo Mujeres.xml'
folder_id = '1a2n_iMJfnnXPjZBOksoXU3UolAEvj-oT'

# Buscar el archivo por nombre en la carpeta de Google Drive
query = f"'{folder_id}' in parents and trashed=false and title='{nombre_archivo}'"
file_list = drive.ListFile({'q': query}).GetList()

# Descargar el contenido del archivo si se encuentra
if len(file_list) > 0:
    xml_file = file_list[0]
    content = xml_file.GetContentString()

    # Transformar el contenido del archivo XML en un DataFrame de Pandas
    root = ET.fromstring(content)
    data = []
    for record in root.findall("./data/record"):
        country = record.find("./field[@name='Country or Area']").text
        year = int(record.find("./field[@name='Year']").text)
        value = record.find("./field[@name='Value']").text
        
        # Agregar valores NaN cuando no hay datos disponibles
        if value is not None:
            value = float(value)
        else:
            value = None
            
        data.append({'country': country, 'year': year, 'desempleo_mujeres_pct': value})
    
    # Crear DataFrame y renombrar columnas
    df = pd.DataFrame(data)
    df_desempleo_mujeres = df.rename(columns={'desempleo_mujeres_pct': 'desempleo_mujeres_pct'})
    
    # Mostrar DataFrame resultante
    #print(df_desempleo_mujeres)
else:
    print(f"No se encontró el archivo '{nombre_archivo}' en la carpeta.")
# Dataframe desempleo hombres 
nombre_archivo = 'Desempleo hombre.xml'
folder_id = '1a2n_iMJfnnXPjZBOksoXU3UolAEvj-oT'

# Buscar el archivo por nombre en la carpeta de Google Drive
query = f"'{folder_id}' in parents and trashed=false and title='{nombre_archivo}'"
file_list = drive.ListFile({'q': query}).GetList()

# Descargar el contenido del archivo si se encuentra
if len(file_list) > 0:
    xml_file = file_list[0]
    content = xml_file.GetContentString()

    # Transformar el contenido del archivo XML en un DataFrame de Pandas
    root = ET.fromstring(content)
    data = []
    for record in root.findall("./data/record"):
        country = record.find("./field[@name='Country or Area']").text
        year = int(record.find("./field[@name='Year']").text)
        value = record.find("./field[@name='Value']").text
        
        # Agregar valores NaN cuando no hay datos disponibles
        if value is not None:
            value = float(value)
        else:
            value = None
            
        data.append({'country': country, 'year': year, 'desempleo_hombres_pct': value})
    
    # Crear DataFrame y renombrar columnas
    df = pd.DataFrame(data)
    df_desempleo_hombres = df.rename(columns={'desempleo_hombres_pct': 'desempleo_hombres_pct'})
    
    # Mostrar DataFrame resultante
    #print(df_desempleo_hombres)
else:
    print(f"No se encontró el archivo '{nombre_archivo}' en la carpeta.")

# Dataframe desempleo total poblacion activa 
nombre_archivo = 'Desempleo total.xml'
folder_id = '1a2n_iMJfnnXPjZBOksoXU3UolAEvj-oT'

# Buscar el archivo por nombre en la carpeta de Google Drive
query = f"'{folder_id}' in parents and trashed=false and title='{nombre_archivo}'"
file_list = drive.ListFile({'q': query}).GetList()

# Descargar el contenido del archivo si se encuentra
if len(file_list) > 0:
    xml_file = file_list[0]
    content = xml_file.GetContentString()

    # Transformar el contenido del archivo XML en un DataFrame de Pandas
    root = ET.fromstring(content)
    data = []
    for record in root.findall("./data/record"):
        country = record.find("./field[@name='Country or Area']").text
        year = int(record.find("./field[@name='Year']").text)
        value = record.find("./field[@name='Value']").text
        
        # Agregar valores NaN cuando no hay datos disponibles
        if value is not None:
            value = float(value)
        else:
            value = None
            
        data.append({'country': country, 'year': year, 'desempleo_total_pct': value})
    
    # Crear DataFrame y renombrar columnas
    df = pd.DataFrame(data)
    df_desempleo_total = df.rename(columns={'desempleo_total_pct': 'desempleo_total_pct'})
    
    # Mostrar DataFrame resultante
    #print(df_desempleo_total)
else:
    print(f"No se encontró el archivo '{nombre_archivo}' en la carpeta.")

#Combinacion de los Dataframe
df_total = pd.merge(df_saldo_cc_pib, df_pib_percapita, on=['country', 'year'])
df_total = pd.merge(df_total, df_inflacion, on=['country', 'year'])
df_total = pd.merge(df_total, df_interest_payment, on=['country', 'year'])
df_total = pd.merge(df_total, df_tasa_interes, on=['country', 'year'])
df_total = pd.merge(df_total, df_tasa_cambio, on=['country', 'year'])
df_total = pd.merge(df_total, df_saldo_cuenta_corriente, on=['country', 'year'])
df_total = pd.merge(df_total, df_deuda_externa, on=['country', 'year'])
df_total = pd.merge(df_total, df_inversion_extranjera_directa_entrada, on=['country', 'year'])
df_total = pd.merge(df_total, df_inversion_extranjera_directa_salida, on=['country', 'year'])
df_total = pd.merge(df_total, df_indice_global_eqity_sp_perct, on=['country', 'year'])
df_total = pd.merge(df_total, df_desempleo_mujeres, on=['country', 'year'])
df_total = pd.merge(df_total, df_desempleo_hombres, on=['country', 'year'])
df_total = pd.merge(df_total, df_desempleo_total, on=['country', 'year'])

#Limpieza del DataframeTotal 

df_total.dropna(subset=['country'], inplace=True)

df_total = df_total.loc[:, ['country','year','PIB_percapita_USD', 'inflacion_porcentual', 'Tasa de Interes', 'tasa_cambio_USD', 'saldo_cuenta_corriente', 'deuda_externa_porcentaje_inb', 'inversion_extranjera_directa_entrada_perct', 'desempleo_total_pct']]

df_total = df_total.loc[df_total['year'] >= 1993]


# Calcula el número de años con valores en blanco para cada país
null_years = df_total.groupby('country').apply(lambda x: x.isnull().sum(axis=0))

# Encuentra los países con al menos tres variables en blanco durante más de cuatro años
null_countries = null_years[(null_years >= 4).sum(axis=1) >= 3].index.tolist()

# Elimina los registros de los países en la lista null_countries
df_total = df_total[~df_total['country'].isin(null_countries)]

#df_total = df_total.loc[df_total['year'] <= 2020]
print(df_total)


df_total.to_csv('./data_normalizada.csv', index=False,  sep = "\t")