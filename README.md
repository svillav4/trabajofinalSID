# Trabajo Final — Sistemas Intensivos en Datos (SI3008)
**Curso:** SI3008 – Sistemas Intensivos en Datos  
**Integrantes:** Samuel Villa (`svillav`) · Felipe Castro Jaimes (`fcastroj`)  
**Repositorio:** https://github.com/svillav4/trabajofinalSID

---

## Descripción general

Este proyecto implementa una plataforma de datos analítica end-to-end para el análisis de tendencias en redes sociales (Instagram, Twitter, Reddit), aplicando los principios de ingesta, transformación y análisis de datos vistos durante el semestre. El pipeline cubre desde la generación simulada de posts hasta la visualización de métricas de engagement, viralidad, toxicidad y tendencias por red social.

El flujo completo es:

```text
generar_post.py / stream_posts.py
↓
Amazon Kinesis Data Streams (social-trends-stream)
↓
Amazon Kinesis Data Firehose (buffer 60s)
↓
S3 raw/social_posts/
↓
AWS Glue Job: glue_transform_raw_to_curated
↓
S3 curated/social_posts/
↓
AWS Glue Job: glue_customer_features
↓
S3 features/social_posts/
↓
AWS Glue Crawler → Glue Data Catalog → Amazon Athena
↓
Consultas SQL exportadas como CSV → Power BI Dashboard
```

---

## Estructura del repositorio

```text
trabajofinalSID/
├── generar_post.py                  # Generador estático de posts simulados
├── stream_posts.py                  # Envío de posts a Kinesis Data Streams
├── dashboardsSID.pbix               # Archivo raw de visualización de Dashboards
├── scripts/
│   └── curated_glue_adapter.py  # Job Glue: raw → curated
│   └── features_glue_adapter.py # Job Glue: curated → features
├── queries/
│   ├── tendencias_por_red_social.sql
│   ├── top_hashtags.sql
│   ├── evolucion_temporal.sql
│   ├── usuarios_activos.sql
│   └── analisis_por_idioma.sql
├── jobs/
│   ├── glue_transform_curated_to_features.py
│   ├── glue_transform_raw_to_curated.py
│   ├── glue_transform_raw_to_curated_a.py
│   ├── step_functions.json
├── arquitectura/
│   ├── Diagrama_arquitectura.drawio.svg
├── requirements.txt
├── .gitignore                  
└── README.md
```

---

## Requisitos previos

- Cuenta AWS Academy con crédito disponible
- AWS CLI configurado con las credenciales del laboratorio
- Python 3.8+ en la máquina local
- Librerías locales: `boto3`, `langdetect`, `better_profanity`
- Power BI Desktop (para visualización)

---

## Instrucciones de despliegue

### 1. Clonar el repositorio

```bash
git clone https://github.com/svillav4/trabajofinalSID.git
cd trabajofinalSID
```

### 2. Crear el bucket S3

En la consola de AWS, crear un bucket con el nombre:
```text
trabajo-final-fcastroj-svillav
```
Dentro del bucket, crear manualmente las siguientes carpetas:
```text
raw/social_posts/
curated/social_posts/
features/social_posts/
scripts/commons/
scripts/curated/
scripts/features/
athena-results/
```
### 3. Subir dependencias a S3

```bash
# Empaquetar langdetect
pip install langdetect -t ./langdetect_pkg
cd langdetect_pkg && zip -r ../langdetect.zip . && cd ..

# Empaquetar better_profanity
pip install better_profanity -t ./profanity_pkg
cd profanity_pkg && zip -r ../better_profanity.zip . && cd ..

# Subir a S3
aws s3 cp langdetect.zip s3://trabajo-final-fcastroj-svillav/scripts/commons/
aws s3 cp better_profanity.zip s3://trabajo-final-fcastroj-svillav/scripts/commons/
aws s3 cp scripts/curated/curated_glue_adapter.py s3://trabajo-final-fcastroj-svillav/scripts/curated/
aws s3 cp scripts/features/features_glue_adapter.py s3://trabajo-final-fcastroj-svillav/scripts/features/
```

### 4. Configurar Kinesis

En la consola de AWS:

**Kinesis Data Stream:**
- Nombre: `social-trends-stream`
- Capacity mode: On-demand

**Kinesis Data Firehose:**
- Source: Amazon Kinesis Data Streams → `social-trends-stream`
- Destination: Amazon S3
- S3 prefix: `raw/social_posts/`
- Buffer interval: 60 segundos
- IAM Role: LabRole

### 5. Crear los Jobs de AWS Glue

**Job 1 — raw → curated:**
- Nombre: `glue_transform_raw_to_curated`
- Script: `s3://trabajo-final-fcastroj-svillav/scripts/curated/curated_glue_adapter.py`
- IAM Role: LabRole
- Glue version: 4.0
- Extra python files: `s3://trabajo-final-fcastroj-svillav/scripts/commons/langdetect.zip`
- Parámetros:

| Key | Value |
|---|---|
| `--input_path` | `s3://trabajo-final-fcastroj-svillav/raw/social_posts/2026/` |
| `--output_path` | `s3://trabajo-final-fcastroj-svillav/curated/social_posts/` |

**Job 2 — curated → features:**
- Nombre: `glue_customer_features`
- Script: `s3://trabajo-final-fcastroj-svillav/scripts/features/features_glue_adapter.py`
- IAM Role: LabRole
- Glue version: 4.0
- Extra python files: `s3://trabajo-final-fcastroj-svillav/scripts/features/features_glue_adapter.py`

- Parámetros:

| Key | Value |
|---|---|
| `--input_path` | `s3://trabajo-final-fcastroj-svillav/curated/social_posts/` |
| `--output_path` | `s3://trabajo-final-fcastroj-svillav/features/social_posts/` |

### 6. Iniciar el pipeline de streaming

```bash
# Instalar dependencias locales
pip install boto3

# Generar y enviar posts al stream
python stream_posts.py
```

Los posts llegarán a S3 raw/ después del buffer de 60 segundos de Firehose.

### 7. Ejecutar los Jobs de Glue

En la consola de AWS Glue, ejecutar en orden:

1. `glue_transform_raw_to_curated` — esperar a que finalice en Succeeded
2. `glue_customer_features` — ejecutar una vez finalice el anterior

La ejecución es secuencial de forma intencional (ver sección de decisiones de diseño más abajo).

### 8. Configurar Athena

En la consola de AWS:

**Glue → Crawlers → Create Crawler:**
- Nombre: `crawler_features_social`
- S3 path: `s3://trabajo-final-fcastroj-svillav/features/social_posts/`
- IAM Role: LabRole
- Target database: `social_trends_db`
- Ejecutar el crawler y esperar a que termine

**Athena → Settings:**
- Query result location: `s3://trabajo-final-fcastroj-svillav/athena-results/`

Ejecutar las consultas disponibles en la carpeta `queries/` del repositorio y exportar los resultados como CSV para cargar en Power BI.

---

## Decisiones de diseño y análisis

### ¿Por qué el pipeline es secuencial y no paralelo?

El pipeline ejecuta los jobs en orden estricto: primero `raw → curated`, luego `curated → features`. Esto responde a dos razones fundamentales.

La primera es una razón técnica de dependencia de datos: el job de features consume como entrada la salida del job de curated. Si ambos corrieran en paralelo, el job de features leería datos incompletos o inexistentes, produciendo un dataset de features vacío o inconsistente. La dependencia entre capas es explícita y no puede obviarse.

La segunda razón es operativa: las cuentas de AWS Academy tienen límites de concurrencia en la ejecución de jobs de Glue. Intentar ejecutarlos en paralelo genera errores de throttling que detienen el pipeline por completo. La ejecución secuencial garantiza estabilidad, trazabilidad y facilita la depuración cuando algo falla, porque el error siempre está aislado en un único job.

En un entorno productivo sin restricciones de cuenta, el diseño podría incorporar paralelismo parcial dentro de cada job (Spark lo maneja internamente por particiones), pero la dependencia entre capas seguiría siendo secuencial por diseño.

### ¿Por qué existe la capa curated y no se pasa directo de raw a features?

La capa raw preserva los datos exactamente como llegan desde Kinesis Firehose: sin limpiar, con posibles duplicados, con nombres de campo dependientes del generador, con timestamps en formatos inconsistentes y con campos nulos. Construir features directamente sobre raw implicaría que cualquier cambio en el formato del generador rompería el job de features, y que los errores de calidad del dato se propagarían silenciosamente hasta el modelo o el dashboard.

La capa curated cumple el rol de zona de limpieza y estandarización: renombra columnas a un esquema estable, estandariza timestamps a UTC, detecta el idioma de cada post, elimina duplicados por `post_id` y filtra registros inválidos. Esta separación garantiza que el job de features siempre recibe datos de calidad conocida, sin importar cómo evolucione el generador upstream.

Este patrón es consistente con arquitecturas medallion (Bronze → Silver → Gold) ampliamente usadas en plataformas de datos modernas, donde cada capa agrega valor incremental y está desacoplada de las anteriores.

### ¿Por qué streaming con Kinesis en lugar de batch directo a S3?

Los datos de redes sociales son inherentemente continuos: los posts se generan en tiempo real a medida que los usuarios interactúan con las plataformas. Cargar estos datos en batch implica necesariamente una ventana de latencia (horas o días) que hace inútil cualquier análisis de tendencias en tiempo cercano al real.

Kinesis Data Streams captura cada evento individual en el momento en que ocurre. Kinesis Firehose los acumula durante 60 segundos y los persiste en S3, lo que balancea la latencia con el costo de operación. Este enfoque permite que el pipeline de batch (Glue) corra sobre datos frescos cada vez que se ejecuta, sin modificar la arquitectura de ingesta.

### ¿Por qué usar AWS Glue con Spark en lugar de otras alternativas?

Se evaluaron varias alternativas para el motor de procesamiento: scripts Python locales, Amazon EMR, AWS Lambda y Amazon Athena directamente. Glue fue seleccionado por tres razones: se integra nativamente con el Glue Data Catalog y S3 sin configuración adicional de red, es serverless por lo que no requiere gestionar clústeres, y soporta PySpark lo que permite escalar el procesamiento horizontalmente si el volumen de datos crece. Para el volumen de datos de este proyecto EMR hubiera sido excesivo, y Lambda tiene un límite de 15 minutos de ejecución que puede ser insuficiente para transformaciones complejas sobre datasets grandes.

### ¿Por qué Power BI en lugar de Amazon QuickSight?

Amazon QuickSight requiere permisos de activación del servicio y configuración de usuario que no están disponibles en cuentas de AWS Academy con LabRole. Como alternativa, los resultados de las consultas de Athena fueron exportados como archivos CSV y cargados en Power BI Desktop, que es gratuito y ofrece capacidades equivalentes de visualización para los propósitos de este proyecto.

---

## Features calculadas

| Feature | Descripción | Fórmula |
|---|---|---|
| `engagement_rate` | Ratio de interacción sobre seguidores | `(likes + comments + saves) / followers` |
| `virality_score` | Proporción de alcance sobre impresiones | `reach / impressions` |
| `is_trending` | Indicador de post trending | `engagement_rate > 0.05` |
| `hour_of_day` | Hora de publicación | Extraída del timestamp UTC |
| `hashtag_count` | Cantidad de hashtags en el post | `size(hashtags)` |
| `avg_hashtag_rank` | Rank promedio de hashtags del post dentro de su plataforma | Join con ranking global por plataforma |
| `best_hashtag_rank` | Mejor rank individual de hashtag del post | `min(hashtag_rank)` |
| `total_posts_by_user` | Total de posts del usuario en el dataset | `count(post_id) GROUP BY user_id` |
| `active_days` | Días distintos en que el usuario publicó | `countDistinct(event_date) GROUP BY user_id` |
| `posting_frequency` | Posts promedio por día activo | `total_posts / active_days` |
| `avg_engagement_per_user` | Engagement promedio histórico del usuario | `avg(engagement_rate) GROUP BY user_id` |
| `toxicity_score` | Score de toxicidad del texto entre 0 y 1 | Ratio palabras ofensivas / total palabras (`better_profanity`) |
| `is_toxic` | Indicador binario de toxicidad | `toxicity_score > 0.1` |
| `detected_language` | Código ISO 639-1 del idioma detectado | `langdetect` sobre el campo `content` |

---

## Consultas de análisis disponibles

Las consultas SQL están en la carpeta `queries/` y se ejecutan directamente en Amazon Athena sobre la tabla `social_trends_db.features_social_posts`:

- `tendencias_por_red_social.sql` — engagement y viralidad promedio por plataforma
- `top_hashtags.sql` — ranking de hashtags más frecuentes por red social
- `evolucion_temporal.sql` — métricas diarias por plataforma para análisis de tendencia
- `usuarios_activos.sql` — usuarios con mayor frecuencia de publicación y su engagement
- `analisis_por_idioma.sql` — distribución de posts y toxicidad por idioma detectado

---

## Consideraciones de costo

El proyecto fue desarrollado con un presupuesto de USD $50 de crédito AWS Academy. Los servicios de mayor consumo son Glue (por tiempo de ejecución de jobs) y Kinesis Data Streams (por hora activo). Para controlar el gasto:

- Apagar los Glue Notebooks inmediatamente después de usarlos
- El Kinesis Data Stream en modo On-demand no cobra si no hay datos fluyendo
- Los archivos en S3 tienen costo marginal (centavos por GB/mes)
- Kinesis Firehose no cobra si no recibe datos

El costo total estimado del proyecto estuvo entre USD $20 y $35.
