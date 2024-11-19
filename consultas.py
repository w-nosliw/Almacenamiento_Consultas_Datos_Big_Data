import happybase
import pandas as pd

# Configuración y conexión con HBase
try:
    # 1. Conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")

    # Nombre de la tabla
    table_name = 'parque_automotor'
    families = {
        'basic': dict(),
        'specs': dict(),
        'status': dict()
    }

    # Eliminar tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)

    # Crear la tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print(f"Tabla '{table_name}' creada exitosamente")

    # 2. Cargar datos desde el CSV
    car_data = pd.read_csv('rows.csv')
    car_data = car_data.head(500)  # Procesar un subconjunto de 500 lineas debido al gran tamaño del conjunto de datos

 # Insertar datos en la tabla
    for index, row in car_data.iterrows():
        row_key = f'vehiculo_{index}'.encode()
        data = {
            b'basic:year_of_registration': str(row['AÑO DE REGISTRO']).encode(),
            b'basic:class': row['CLASE'].encode(),
            b'basic:make': row['MARCA'].encode(),
            b'basic:model_year': str(row['MODELO']).encode(),
            b'basic:car_body': row['CARROCERÍA'].encode(),
            b'specs:engine_capacity': str(row['CILINDRAJE']).encode(),
            b'specs:passenger_capacity': str(row['CAPACIDAD DE PASAJEROS']).encode(),
            b'specs:cargo_capacity': str(row['CAPACIDAD DE CARGA']).encode(),
            b'specs:fuel_type': row['TIPO DE COMBUSTIBLE'].encode(),
            b'status:weight': str(row['PESO']).encode(),
            b'status:soat_valid': row['SOAT VIGENTE'].encode(),
            b'status:emissions_cert_valid': row['GASES VIGENTE'].encode(),
            b'status:service_type': row['SERVICIO'].encode()
        }
        table.put(row_key, data)

    print("Datos cargados exitosamente")

    # 3. Consultas y Filtrado
    print("\n=== Consultas sobre los datos ===")

    # Recorrer todos los registros (mostrar solo 5 primeros)
    print("\nPrimeros 5 registros:")
    count = 0
    for key, data in table.scan():
        if count < 5:
            print(f"\nID: {key.decode()}")
            print(f"Año de Registro: {data[b'basic:year_of_registration'].decode()}")
            print(f"Marca: {data[b'basic:make'].decode()}")
        count += 1

# Filtrar por una condición: Vehículos de un año específico
    print("\nVehículos registrados en 2018:")
    for key, data in table.scan():
        if data[b'basic:year_of_registration'].decode() == '2018':
            print(f"ID: {key.decode()}, Marca: {data[b'basic:make'].decode()}")

    # Filtrar por otro criterio: Tipo de combustible
    print("\nVehículos que usan gasolina:")
    for key, data in table.scan():
        if data[b'specs:fuel_type'].decode().lower() == 'gasolina':
            print(f"ID: {key.decode()}, Marca: {data[b'basic:make'].decode()}")

    # 4. Escritura: Inserción, Actualización y Eliminación
    print("\n=== Operaciones de Escritura ===")

    # Inserción: Agregar un nuevo registro
    print("Insertando un nuevo vehículo...")
    new_vehicle = {
        b'basic:year_of_registration': b'2020',
        b'basic:class': b'Automovil',
        b'basic:make': b'Toyota',
        b'basic:model_year': b'2020',
        b'basic:car_body': b'Sedan',
        b'specs:engine_capacity': b'1800',
        b'specs:passenger_capacity': b'5',
        b'specs:cargo_capacity': b'450',
        b'specs:fuel_type': b'Gasolina',
        b'status:weight': b'1200',
        b'status:soat_valid': b'Si',
        b'status:emissions_cert_valid': b'Si',
        b'status:service_type': b'Particular'
    }
    table.put(b'vehiculo_nuevo', new_vehicle)
    print("Vehículo insertado con éxito.")
    print(new_vehicle)

# Actualización: Cambiar el modelo de un vehículo existente
    print("\nActualizando el modelo de un vehículo...")
    updated_model = {b'basic:model_year': b'2021'}
    table.put(b'vehiculo_0', updated_model)
    updated_data = table.row(b'vehiculo_0')
    print("Modelo actualizado exitosamente.")
    print(updated_data)

    # Eliminación: Borrar un registro
    print("\nEliminando un vehículo...")
    deleted_vehicle = table.row(b'vehiculo_0')
    table.delete(b'vehiculo_0')
    print("Vehículo eliminado con éxito.")
    print(deleted_vehicle)

except Exception as e:
    print(f"Error: {str(e)}")

finally:
    # Cerrar la conexión
    connection.close()
    print("Conexión con HBase cerrada.")
