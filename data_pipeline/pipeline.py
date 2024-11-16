import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
from config import DATABASE_CONFIG, CSV_FILES, LOG_FILE

#Configuracion de logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_db_engine(DATABASE_CONFIG):
    """
    Crea una conexion de motor a la base de datos MYSQL
    """
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"
        )
        logging.info("Conexion a la base de datos establecida correctamente")
        return engine
    except Exception as e :
        logging.error(f"Error al conectar a la base de datos: {e}")
        sys.exit(1)

def validate_ids(df, valid_ids, col):
    """
    Valida relaciones entre DataFrames
    """
    if not df[col].isin(valid_ids).all():
        logging.error(f"hay {col} que no se encuentran en el otro dataframe")
        sys.exit(1)

def read_csv(file_path):
    """ 
    Lee un archivo CSV y devuelve un dataframe de pandas
    """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Archivo {file_path} leido exitosamente")
        return df
    except Exception as e:
        logging.error(f"Error al leer el archivo {file_path}: {e}")

def transform_customers(df):
    """
    Realiza transformaciones especificas en el DataFrame de customers
    """

    df['customer_email'] = df['customer_email'].str.lower()
    
    #Validar campos obligatorios
    if df[['customer_fname', 'customer_lname', 'customer_email']].isnull().any().any():
        logging.error("Datos faltantes en el DataFrame de customers")
        sys.exit(1)
    
    return df

def transform_departments(df):
    """
    Realiza la validacion del campo department_name
    """
    
    if df['department_name'].duplicated().any():
        logging.warning("Hay departamentos duplicados en el DataFrame df_departments")
        sys.exit(1)

    return df

def transform_categories(df, df_departments):
    """
    Realiza validaciones en el DataFrame de Categorias
    """
    valid_ids = set(df_departments['department_id'])

    validate_ids(df, valid_ids, 'category_department_id' )

    return df

def transform_poducts(df, df_categories):
    """
    Realiza validaciones en el DataFrame de productos
    """
    valid_ids = set(df_categories['category_id'])
    # Valida que product_category_id exista en categories
    validate_ids(df, valid_ids, 'product_category_id')
    return df

def transform_orders(df, df_customers):
    """
    Realiza transformaciones en el DataFrame de Ordenes
    """
    #Convertir order_date a datetime
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    #Validar si hay valores nulos
    if df['order_date'].isnull().any():
        logging.erro("Hay valores invalidos en order_date")
        sys.exit(1)
    
    valid_ids = set(df_customers['customer_id'])

    validate_ids(df, valid_ids, 'order_customer_id')
    
    return df

def transform_order_item(df, df_orders, df_products):
    """
    Realiza transformaciones y validaciones en el DataFrame de order_items
    """
    valid_order_ids = set(df_orders['order_id'])
    valid_product_ids = set(df_products['product_id'])

    validate_ids(df,valid_order_ids, 'order_item_order_id' )
    validate_ids(df, valid_product_ids, 'order_item_product_id')

    #Calcular order_item_subtotal, si no esta presente o este incorrecto
    calcular_subtotal = df['order_item_quantity'] * df['order_item_product_price']
    if not (df['order_item_subtotal'] == calcular_subtotal).all():
        logging.info("Recalculando order_item_subtotal")
        df['order_item_subtotal'] = calcular_subtotal

    return df

def load_data(engine, table_name, df):
    """
    Carga un DataFRame de pandas a una tabla en MySQL
    """
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logging.info(f"Datos cargados exitosamente en la tabla {table_name}")
    except Exception as e:
        logging.error(f"Error al cargar la data en la tabla {table_name}")
        sys.exit(1)


def main():

    # Crear la conexion a la base datos
    engine = create_db_engine(DATABASE_CONFIG)

    load_order = ["departments", "categories", "customers", "products", "orders", "order_items"]

    # Diccionario para almacenar DatFrames transformados

    dataframes = {}

    # Carga de datos y transformaciones

    # 1. departments
    df_departments = read_csv(CSV_FILES['departments'])
    df_departments = transform_departments(df_departments)
    dataframes['departments'] = df_departments

    # 2. Categories

    df_categories = read_csv(CSV_FILES['categories'])
    df_categories = transform_categories(df_categories, df_departments)
    dataframes['categories'] = df_categories

    # 3. Customers

    df_customers = read_csv(CSV_FILES['customers'])
    df_customers = transform_customers(df_customers)
    dataframes['customers'] = df_customers

    # 4. Products

    df_products = read_csv(CSV_FILES['products'])
    df_products = transform_poducts(df_products, df_categories)
    dataframes['products'] = df_products

    # 5. Orders
    df_orders = read_csv(CSV_FILES['orders'])
    df_orders = transform_orders(df_orders, df_customers)
    dataframes['orders'] = df_orders

    # 6. Oorder_items

    df_order_items = read_csv(CSV_FILES['order_items'])
    df_order_items = transform_order_item(df_order_items, df_orders, df_products)
    dataframes['order_items'] = df_order_items

    for table in load_order:
        load_data(engine, table, dataframes[table])
    
    logging.info("Pipeline de datos ejecutado existosamente")

if __name__  == "__main__":
    main()


