import dlt
from pyspark.sql.functions import (
    col, current_timestamp, lit, trim, 
    regexp_replace, to_date, when, coalesce, avg, count, sum
)

VOLUME_PATH = "/Volumes/electrocasa/bronze/landing_volume"

# ==========================================
# 1. CAPA BRONZE (Ingesta cruda desde archivos en volumen)
# ==========================================

@dlt.table(name="bronze_ventas", comment="Ingesta raw de ventas por sucursal")
def bronze_ventas():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(f"{VOLUME_PATH}/ventas_sucursales.csv")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

@dlt.table(name="bronze_catalogo", comment="Snapshot maestro de productos")
def bronze_catalogo():
    return (
        spark.read.format("json")
        .load(f"{VOLUME_PATH}/catalogo_productos.json")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

@dlt.table(name="bronze_empleados", comment="Histórico crudo de RR.HH.")
def bronze_empleados():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(f"{VOLUME_PATH}/empleados_rrhh.csv")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

@dlt.table(name="bronze_resenas", comment="Reseñas semiestructuradas de clientes")
def bronze_resenas():
    return (
        spark.read.format("json")
        .load(f"{VOLUME_PATH}/resenas_clientes.json")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

@dlt.table(name="bronze_devoluciones", comment="Registro crudo de devoluciones")
def bronze_devoluciones():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .load(f"{VOLUME_PATH}/devoluciones.csv")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

@dlt.table(name="bronze_tracking", comment="Tracking de envíos courier temporal")
def bronze_tracking():
    return spark.read.table("electrocasa.bronze.tracking_raw")

# ==========================================
# 2. CAPA SILVER (Limpieza, Calidad y Reglas)
# ==========================================

@dlt.table(name="silver_ventas", comment="Ventas limpias con validación crítica")
@dlt.expect_or_drop("valid_monto_total", "monto_total > 0")
def silver_ventas():
    df = dlt.read("bronze_ventas")
    fecha_parsed = coalesce(
        to_date(col("fecha_venta"), "yyyy-MM-dd"),
        to_date(col("fecha_venta"), "dd/MM/yyyy")
    )
    return (
        df.filter(col("venta_id").isNotNull())
        .withColumn("monto_total", col("monto_total").cast("double"))
        .withColumn("cantidad", col("cantidad").cast("int"))
        .withColumn("fecha_venta", fecha_parsed)
        .withColumn("metodo_pago", trim(col("metodo_pago")))
    )

@dlt.table(name="silver_ventas_cuarentena", comment="Registros de ventas rechazados por calidad")
def silver_ventas_cuarentena():
    return (
        dlt.read("bronze_ventas")
        .filter("(monto_total <= 0) OR (monto_total IS NULL)")
        .withColumn("motivo_rechazo", lit("Monto total nulo o menor/igual a 0"))
        .withColumn("fecha_cuarentena", current_timestamp())
    )

@dlt.table(name="silver_catalogo", comment="Catálogo estandarizado sin símbolos de moneda")
@dlt.expect_or_drop("valid_precio_lista", "precio_lista > 0")
def silver_catalogo():
    precio_limpio = regexp_replace(col("precio_lista"), r"[^\d.]", "").cast("double")
    return (
        dlt.read("bronze_catalogo")
        .filter(col("producto_id").isNotNull())
        .withColumn("precio_lista", precio_limpio)
        .withColumn("categoria", trim(col("categoria")))
        .withColumn("marca", coalesce(trim(col("marca")), lit("Generico")))
    )

@dlt.table(name="silver_resenas", comment="Reseñas con expectativas de calificación en rango")
@dlt.expect("calificacion_en_rango", "calificacion >= 1 AND calificacion <= 5")
def silver_resenas():
    return (
        dlt.read("bronze_resenas")
        .filter(col("resena_id").isNotNull())
        .withColumn("calificacion", col("calificacion").cast("int"))
        .withColumn("fecha_resena", to_date(col("fecha_resena"), "yyyy-MM-dd"))
    )

@dlt.table(name="silver_empleados", comment="Dimensión historizada de personal")
@dlt.expect_or_drop("dni_no_nulo", "dni IS NOT NULL")
def silver_empleados():
    return (
        dlt.read("bronze_empleados")
        .filter(col("id_empleado").isNotNull())
        .withColumn("salario", col("salario").cast("double"))
    )

@dlt.table(name="silver_tracking", comment="Tracking de envíos estandarizado")
@dlt.expect("estado_valido", "estado_entrega IN ('ENTREGADO', 'EN_CAMINO', 'PENDIENTE', 'DEVUELTO')")
def silver_tracking():
    estado_normalizado = when(col("estado_entrega").isin("Entregado", "entregado", "ENTREGADO"), "ENTREGADO") \
        .when(col("estado_entrega").isin("En camino", "EN_CAMINO", "en_transito"), "EN_CAMINO") \
        .when(col("estado_entrega").isin("Pendiente", "PENDIENTE"), "PENDIENTE") \
        .when(col("estado_entrega").isin("Devuelto", "DEVUELTO"), "DEVUELTO") \
        .otherwise("PENDIENTE")
        
    return (
        dlt.read("bronze_tracking")
        .withColumn("estado_entrega", estado_normalizado)
        .withColumn("courier", trim(col("courier")))
    )

# ==========================================
# 3. CAPA GOLD (Data Marts analíticos de negocio)
# ==========================================

@dlt.table(name="gold_ventas_sucursal_mes", comment="Ventas y ticket promedio por sucursal")
def gold_ventas_sucursal_mes():
    return (
        dlt.read("silver_ventas")
        .groupBy("sucursal_id", "fecha_venta")
        .agg(
            sum("monto_total").alias("total_ventas"),
            avg("monto_total").alias("ticket_promedio"),
            count("venta_id").alias("total_transacciones")
        )
    )

@dlt.table(name="gold_resenas_categoria", comment="Tasa de reseñas negativas por categoría")
def gold_resenas_categoria():
    resenas = dlt.read("silver_resenas")
    catalogo = dlt.read("silver_catalogo")
    
    return (
        resenas.join(catalogo, "producto_id", "inner")
        .groupBy("categoria")
        .agg(
            avg("calificacion").alias("promedio_calificacion"),
            count(when(col("calificacion") <= 2, 1)).alias("total_resenas_negativas"),
            count("resena_id").alias("total_resenas")
        )
    )