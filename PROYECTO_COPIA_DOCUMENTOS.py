# -*- coding: utf-8 -*-
"""
Script para listar archivos en una carpeta específica de Google Drive,
ingresando a todas sus subcarpetas, y comparar el resultado actual con un snapshot
guardado en Google Sheets. Solo se registran los cambios (nuevos y eliminados) en una hoja
nueva, cuyo nombre es la fecha y hora de la consulta.
El script incorpora:
  - Paginación en la API de Drive
  - Inserción en lotes (batching) en Google Sheets
  - Manejo de excepciones y logging
  - Modularización en funciones
  - Comparación con snapshot previo para detectar cambios
"""

import logging
import time
from datetime import datetime

import gspread
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Configurar logging para seguimiento y depuración
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuración de credenciales y alcance
SERVICE_ACCOUNT_FILE = "united-concord-451002-u8-73e136ba2e77.json"  # Asegúrate de que este archivo existe o usa la ruta completa
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

def obtener_servicios():
    """Obtiene los servicios de Google Drive y Google Sheets."""
    try:
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=creds)
        sheets_client = gspread.authorize(creds)
        return drive_service, sheets_client
    except Exception as e:
        logging.error("Error al obtener servicios: %s", e)
        raise

def obtener_archivos_recursivamente(drive_service, parent_id, ruta_actual="Carpeta Principal"):
    """
    Busca de forma recursiva archivos y carpetas dentro de la carpeta con id `parent_id`.
    Devuelve una lista de registros con:
      [Ubicación, Nombre, Tipo, Propietario, Fecha de Subida]
    Incorpora paginación para manejar grandes cantidades de archivos.
    """
    archivos_lista = []
    page_token = None
    while True:
        try:
            resultados = drive_service.files().list(
                q=f"'{parent_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name, mimeType, owners, createdTime)",
                pageToken=page_token
            ).execute()
        except Exception as e:
            logging.error("Error al listar archivos en la carpeta %s: %s", parent_id, e)
            break

        items = resultados.get("files", [])
        for item in items:
            tipo = "Carpeta" if item["mimeType"] == "application/vnd.google-apps.folder" else "Archivo"
            propietario = item["owners"][0]["emailAddress"] if "owners" in item and item["owners"] else "Desconocido"
            fecha_subida = item.get("createdTime", "No disponible")
            try:
                fecha_obj = datetime.strptime(fecha_subida, "%Y-%m-%dT%H:%M:%S.%fZ")
                fecha_formateada = fecha_obj.strftime("%d/%m/%Y %H:%M:%S")
            except Exception as e:
                logging.warning("No se pudo formatear la fecha %s: %s", fecha_subida, e)
                fecha_formateada = fecha_subida

            archivos_lista.append([ruta_actual, item["name"], tipo, propietario, fecha_formateada])
            # Si es carpeta, buscar dentro de ella de forma recursiva
            if tipo == "Carpeta":
                nueva_ruta = f"{ruta_actual}/{item['name']}"
                archivos_lista.extend(obtener_archivos_recursivamente(drive_service, item["id"], nueva_ruta))
        page_token = resultados.get("nextPageToken", None)
        if not page_token:
            break
    return archivos_lista

def insertar_datos_en_sheets(sheet, datos, batch_size=100):
    """
    Inserta datos en Google Sheets en lotes para reducir el número de solicitudes.
    """
    total = len(datos)
    for i in range(0, total, batch_size):
        lote = datos[i:i+batch_size]
        try:
            sheet.append_rows(lote, value_input_option='USER_ENTERED')
            logging.info("Insertado lote de filas %d a %d", i+1, i+len(lote))
            time.sleep(1)  # Pausa breve para evitar exceder la cuota
        except Exception as e:
            logging.error("Error insertando lote de filas: %s", e)

def obtener_snapshot(snapshot_sheet):
    """
    Lee el snapshot actual desde la hoja 'Snapshot' y lo devuelve como un diccionario
    con clave = Ubicación + "_" + Nombre.
    """
    data = snapshot_sheet.get_all_values()
    if len(data) < 2:
        return {}  # No hay datos previos
    rows = data[1:]  # Omite encabezado
    snapshot = {}
    for row in rows:
        # Asumiendo que las columnas son: Ubicación, Nombre, Tipo, Propietario, Fecha de Subida
        key = row[0] + "_" + row[1]
        snapshot[key] = row
    return snapshot

def comparar_snapshot(snapshot_actual, snapshot_nuevo):
    """
    Compara el snapshot nuevo (diccionario) con el snapshot actual.
    Retorna dos listas: [nuevos_archivos, eliminados].
    """
    nuevos = []
    eliminados = []
    # Comparar claves
    for key, row in snapshot_nuevo.items():
        if key not in snapshot_actual:
            nuevos.append(row)
    for key, row in snapshot_actual.items():
        if key not in snapshot_nuevo:
            eliminados.append(row)
    return nuevos, eliminados

def registrar_cambios(spreadsheet, nuevos, eliminados):
    """
    Crea una nueva hoja en el spreadsheet, cuyo nombre es la fecha y hora de la consulta,
    y registra los cambios encontrados: archivos nuevos y eliminados.
    """
    nombre_sheet = "Cambios " + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    try:
        cambios_sheet = spreadsheet.add_worksheet(title=nombre_sheet, rows="1000", cols="10")
    except Exception as e:
        logging.error("Error al crear la hoja de cambios: %s", e)
        return

    header = ["Tipo de Cambio", "Ubicación", "Nombre", "Tipo", "Propietario", "Fecha de Subida"]
    cambios_sheet.append_row(header)
    # Registrar nuevos
    for row in nuevos:
        cambios_sheet.append_row(["Nuevo"] + row)
    # Registrar eliminados
    for row in eliminados:
        cambios_sheet.append_row(["Eliminado"] + row)
    logging.info("Se han registrado cambios en la hoja '%s'.", nombre_sheet)

def actualizar_snapshot(snapshot_sheet, datos):
    """
    Actualiza la hoja 'Snapshot' con los datos actuales.
    """
    snapshot_sheet.clear()
    header = ["Ubicación", "Nombre", "Tipo", "Propietario", "Fecha de Subida"]
    snapshot_sheet.append_row(header)
    snapshot_sheet.append_rows(datos)
    logging.info("Snapshot actualizado con %d registros.", len(datos))

def main():
    try:
        drive_service, sheets_client = obtener_servicios()
    except Exception as e:
        logging.error("No se pudieron obtener los servicios necesarios: %s", e)
        return

    # Configurar IDs: carpeta de inicio en Drive y spreadsheet
    FOLDER_ID = "1mDdBuS9EjjO1iXsq2l99WJgj0KEmZiPt"  # Carpeta de Google Drive
    SHEET_ID = "1MGHuyIgHoFgmFmZ5g0AXFrwK2Z0S5-rxEeDRbld9MCU"  # Spreadsheet de Google Sheets

    logging.info("Obteniendo archivos de forma recursiva desde la carpeta %s...", FOLDER_ID)
    datos_actuales = obtener_archivos_recursivamente(drive_service, FOLDER_ID)
    logging.info("Se encontraron %d registros actualmente.", len(datos_actuales))

    # Abrir el spreadsheet
    try:
        spreadsheet = sheets_client.open_by_key(SHEET_ID)
        main_sheet = spreadsheet.sheet1  # Hoja principal (no se modificará en esta operación)
    except Exception as e:
        logging.error("Error al abrir el spreadsheet: %s", e)
        return

    # Acceder (o crear) la hoja 'Snapshot' para tener referencia
    try:
        snapshot_sheet = spreadsheet.worksheet("Snapshot")
    except Exception:
        snapshot_sheet = spreadsheet.add_worksheet(title="Snapshot", rows="1000", cols="10")
        # Si es la primera vez, guardamos el snapshot y no hay cambios
        snapshot_sheet.append_row(["Ubicación", "Nombre", "Tipo", "Propietario", "Fecha de Subida"])
        snapshot_sheet.append_rows(datos_actuales)
        logging.info("Se ha creado la hoja 'Snapshot'. No hay cambios previos.")
        return

    # Leer el snapshot actual (referencia)
    snapshot_actual = obtener_snapshot(snapshot_sheet)

    # Convertir el snapshot actual y los datos actuales a diccionarios clave-valor
    snapshot_nuevo = {}
    for row in datos_actuales:
        key = row[0] + "_" + row[1]
        snapshot_nuevo[key] = row

    # Comparar para detectar cambios
    nuevos_archivos, eliminados = comparar_snapshot(snapshot_actual, snapshot_nuevo)
    logging.info("Detectados %d nuevos archivos y %d eliminados.", len(nuevos_archivos), len(eliminados))

    # Si se encontraron cambios, registrar en una hoja nueva con fecha y hora
    if nuevos_archivos or eliminados:
        registrar_cambios(spreadsheet, nuevos_archivos, eliminados)
    else:
        logging.info("No se detectaron cambios.")

    # Actualizar el snapshot para futuras comparaciones
    actualizar_snapshot(snapshot_sheet, datos_actuales)

if __name__ == "__main__":
    main()
