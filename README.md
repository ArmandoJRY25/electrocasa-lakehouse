# ElectroCasa Lakehouse - Plataforma de Datos

## 1. Contexto del Negocio y Caso de Uso
**ElectroCasa** es una cadena minorista de electrodomésticos con 40 sucursales en Perú (Lima y provincias). Opera a través de tres canales de venta (tienda física, ecommerce y despacho por courier externo). El objetivo de este proyecto es centralizar fuentes de datos heterogéneas (archivos planos diarios, JSON semiestructurados de reseñas, historiales de RR.HH. y una base de datos relacional externa de tracking) en una única plataforma analítica basada en la arquitectura **Lakehouse** sobre Azure Databricks.

---

## 2. Arquitectura Medallion y Diseño del Pipeline
El flujo de datos se implementa mediante **Lakeflow Declarative Pipelines (DLT)** estructurado en tres capas:

- **Capa Bronze (Raw Zone)**: 
  - Ingesta cruda y sin alteración de negocio de los archivos de ventas, catálogo, reseñas, devoluciones y empleados alojados en Unity Catalog Volumes (`/Volumes/electrocasa/bronze/landing_volume`).
  - Ingesta federada de la tabla `TrackingEnvios` desde Azure SQL Database utilizando **Lakehouse Federation** (`electrocasa_sql_source.dbo.TrackingEnvios`) sin duplicar almacenamiento físico ni exponer credenciales en texto plano.
  - Incorporación de columnas técnicas de auditoría (`_ingestion_timestamp`, `_source_file`).

- **Capa Silver (Cleansed & Quality Zone)**:
  - Limpieza, estandarización de tipos con `try_cast`, normalización de textos (`trim`) y parseo de fechas.
  - **Calidad de datos**: Aplicación de reglas críticas de negocio mediante `@dlt.expect_or_drop` (como montos de ventas válidos y precios positivos) y redirección de registros corruptos hacia una **tabla de cuarentena** (`silver_ventas_cuarentena`) para trazabilidad de auditoría.
  - **Historización (SCD Tipo 2)**: Procesamiento de la dimensión de empleados para conservar el historial de cambios de sucursal, cargos y salarios ante altas y transferencias.

- **Capa Gold (Data Marts Analíticos)**:
  - Agregaciones de negocio orientadas a responder las preguntas clave de la compañía:
    - Ventas totales y ticket promedio por sucursal y mes (`gold_ventas_sucursal_mes`).
    - Tasa de reseñas negativas por categoría de producto (`gold_resenas_categoria`).
    - Dotación activa de personal por sucursal (`gold_dotacion_sucursal`).

---

## 3. Justificación Técnica

### A. Métodos de Ingesta Heterogénea
1. **Volúmenes de Unity Catalog (Batch / Micro-batch)**: Ideal para archivos diarios y catálogos estáticos entregados por las sucursales (CSV/JSON), garantizando idempotencia y trazabilidad de origen.
2. **Lakehouse Federation (Azure SQL)**: Utilizado para el tracking de envíos externo. Permite consultar la base de datos relacional en tiempo real de manera declarativa y gobernada a través del Unity Catalog, minimizando costos de replicación masiva y cumpliendo con los estándares de seguridad corporativa.

### B. Historización en Empleados
Se implementó historización de cambios (SCD Tipo 2) para el personal de RR.HH. Esto permite registrar modificaciones salariales y traslados entre sucursales sin sobrescribir el registro histórico, asegurando que las auditorías de nómina y la dotación activa de personal reflejen fielmente el estado vigente en cada período.

### C. Estrategia de Cómputo y Costos (FinOps)
- **DLT Serverless**: Seleccionado para la ejecución del pipeline Medallion. Reduce la sobrecarga operativa, escala de manera automática según el volumen de los micro-lotes diarios y optimiza costos al no mantener clústeres inactivos en espera.
- **Job Clusters (Databricks Workflows)**: Utilizados para las tareas programadas de orquestación, garantizando aislamiento de recursos y control predecible del presupuesto asignado a la ejecución diaria del pipeline.

---

## 4. Gobierno y Seguridad (Unity Catalog)
- **Esquemas por Capa**: Organización estructurada en `bronze`, `silver` y `gold` bajo el catálogo exclusivo `electrocasa`.
- **Control de Accesos Basado en Roles (RBAC)**: Creación de grupos organizacionales (`ingenieria`, `analistas`, `auditoria`) con privilegios diferenciados (`GRANT`/`REVOKE`).
- **Dynamic Data Masking**: Implementación de la función `electrocasa.silver.mask_dni` basada en membresía de grupo (`IS_ACCOUNT_GROUP_MEMBER`), la cual oculta los datos sensibles de la columna DNI (`***-***-XX`) a cualquier usuario que no pertenezca al área de ingeniería.

---

## 5. Pasos de Despliegue con Databricks Asset Bundles (DAB)

El proyecto está parametrizado para múltiples entornos (`dev` y `prod`) mediante infraestructura como código:

1. **Configurar las credenciales o el entorno local de DAB**:
   ```bash
   databricks auth login --host https://<tu-workspace>.cloud.databricks.com