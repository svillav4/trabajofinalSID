import random
import uuid
from datetime import datetime, timedelta

PALABRAS_ES = [
    "increíble", "viaje", "comida", "tecnología", "música", "deporte",
    "feliz", "triste", "lunes", "noche", "ciudad", "trabajo", "amigos",
    "café", "lluvia", "sol", "perro", "libro", "código", "startup",
    "python", "datos", "nube", "IA", "modelo", "red", "post", "foto",
    "video", "live", "tendencia", "viral", "comentario", "like", "share",
]

PALABRAS_EN = [
    "awesome", "travel", "food", "tech", "music", "sport", "happy",
    "monday", "night", "city", "work", "friends", "coffee", "rain",
    "sunshine", "dog", "book", "code", "startup", "python", "data",
    "cloud", "AI", "model", "network", "post", "photo", "video",
    "trending", "viral", "comment", "like", "share", "throwback",
]

HASHTAGS = [
    "#TBT", "#instagood", "#python", "#datascience", "#glue", "#aws",
    "#machinelearning", "#Colombia", "#Medellín", "#vibes", "#trending",
    "#coding", "#NoCode", "#LowCode", "#ETL", "#BigData", "#openai",
    "#startup", "#travel", "#foodie", "#MondayMotivation", "#QEPD",
    "#RIP", "#breaking", "#news", "#throwbackthursday", "#selfie",
    "#workout", "#fitnessmotivation", "#love", "#life", "#happy",
]

EMOJIS = [
    "😂", "🔥", "💯", "🚀", "😍", "👀", "🤔", "😅", "🙏", "💀",
    "🎉", "😤", "🤯", "👏", "💪", "🌍", "📊", "🐍", "☕", "🌧️",
    "😎", "🥳", "😭", "😩", "🤦", "🙄", "✨", "⚡", "🎯", "📌",
    "❓", "❗", "‼️", "⁉️", "✅", "❌", "🔴", "🟢", "🔵", "⭐",
]

USUARIOS = [
    "juan_perez", "MariaLopez99", "TECHGURU_2024", "datos_con_cafe",
    "el_programador", "LauraXXX", "rodrigo.dev", "snake_charmer_py",
    "NOMAD_DIGITAL", "futbol_fan_co", "ana_maria_123", "DevOpsHero",
    "glue_tester", "Mr_DataEngineer", "coffeeCODER", "REALUSER_2023",
    "invisible_hand", "night_owl_dev", "superadmin", "user_42",
]

SUBREDDITS = [
    "r/datascience", "r/Python", "r/worldnews", "r/Colombia", "r/learnprogramming",
    "r/MachineLearning", "r/AskReddit", "r/funny", "r/technology", "r/aws",
    "r/ETL", "r/dataengineering", "r/startups", "r/investing", "r/memes",
]

TITULOS_REDDIT = [
    "Por qué nadie habla de esto???",
    "AYUDA urgente con AWS Glue [RESUELTO]",
    "El mejor tutorial de Python que encontré (gratis)",
    "HOY aprendí algo que cambió mi vida como dev",
    "¿Alguien más tiene este problema con pySpark?",
    "Miren esto que encontré... increíble o no??",
    "Mi empresa migró todo a la nube y esto pasó...",
    "Pregunta estúpida pero... ¿cómo limpian datos en producción?",
    "Top 10 errores de principiantes en SQL (el #7 me pasó hoy)",
    "Rant: por qué los datos nunca vienen limpios 😤",
]

CUERPOS = [
    "acabo de descubrir que {palabra} es {adjetivo}!!! {emoji} {emoji}",
    "HOY en el trabajo tuvimos un problema CON {palabra} y {palabra2}... al final {resolucion}",
    "¿alguien sabe cómo hacer {palabra} sin morir en el intento? pregunto para un amigo {emoji}",
    "llevaba {num} horas tratando de {accion} y resulta que era un {error_tipico} 🤦",
    "Reminder: {consejo}. De nada. {emoji}",
    "no entiendo por qué la gente {queja}??? {emoji}{emoji}",
    "Thread: cosas que aprendí sobre {tema} esta semana 🧵👇",
    "Update: {actualizacion}. Más info pronto.",
    "Buen {momento_dia} a todos menos a {excepcion} {emoji}",
    "Si usas {herramienta} y no sabes {tip}, te estás perdiendo la vida",
    "{frase_inicio} y luego {frase_fin} {emoji}",
    "lokita la vida {emoji}{emoji}{emoji} ayer {evento_random}",
    "Alerta {nivel_urgencia}: {descripcion_random}",
    "data SUCIA >> datos limpios >> {resultado} {emoji}",
]

ADJETIVOS = ["increíble", "terrible", "random", "GENIAL", "roto", "lento", "útil", "INÚTIL"]
RESOLUCIONES = ["lo arreglamos", "quedó peor", "reiniciamos el servidor", "era un ; faltante"]
ACCIONES = ["parsear JSON", "limpiar strings", "correr el job de Glue", "conectar a S3"]
ERRORES_TIPICOS = ["typo en el nombre del campo", "encoding UTF-8", "null pointer", "schema mismatch"]
CONSEJOS = [
    "valida el schema ANTES de correr el job",
    "siempre haz backup antes de transformar",
    "los logs son tus amigos",
    "documentar no es opcional",
]
QUEJAS = [
    "manda datos sin header",
    "usa espacios en nombres de columnas",
    "mezcla formatos de fecha",
    "hardcodea credenciales",
]
TEMAS = ["ETL", "Python", "SQL", "AWS", "machine learning", "APIs REST"]
HERRAMIENTAS = ["pandas", "Glue", "Spark", "dbt", "Airflow", "Kafka"]
TIPS = ["los DynamicFrames", "el crawler", "el job bookmark", "los pushdown predicates"]
MOMENTOS = ["lunes", "martes", "miércoles", "día", "viernes", "fin de semana"]
EXCEPCIONES = ["los nulls en producción", "el cliente que manda csvs rotos", "el encoding latin-1"]
ACTUALIZACIONES = [
    "el pipeline ya corre en menos de 5 min",
    "migramos 10TB sin perder un registro",
    "el modelo ya está en producción",
]
EVENTOS_RANDOM = [
    "me cayó un NULL en prod a las 3am",
    "el cliente mandó el archivo en xlsx sin headers",
    "AWS cobró el doble de lo esperado",
    "encontré un bug de hace 3 años en el código",
]
NIVELES_URGENCIA = ["⚠️ MEDIA", "🔴 CRÍTICA", "🟡 BAJA", "❗ IMPORTANTE"]
DESCRIPCIONES = [
    "pipeline caído en ambiente de producción",
    "schema inesperado en fuente de datos",
    "job de Glue corriendo por más de 2h",
    "inconsistencia detectada en tabla de hechos",
]

def _palabras_random(n=3):
    pool = PALABRAS_ES + PALABRAS_EN
    return " ".join(random.choices(pool, k=n))


def _hashtags_random(n=None):
    n = n or random.randint(0, 5)
    return " ".join(random.sample(HASHTAGS, min(n, len(HASHTAGS))))


def _emojis_random(n=None):
    n = n or random.randint(1, 4)
    return "".join(random.choices(EMOJIS, k=n))


def _timestamp_random():
    base = datetime(2023, 1, 1)
    delta = timedelta(
        days=random.randint(0, 730),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return (base + delta).isoformat()


def _aplicar_ruido_texto(texto):
    ruido = random.random()
    if ruido < 0.15:
        texto = texto.upper()
    elif ruido < 0.30:
        texto = texto.lower()
    elif ruido < 0.45:
        texto = "".join(c.upper() if random.random() > 0.5 else c.lower() for c in texto)
    # Espacios extra al inicio/final
    if random.random() < 0.25:
        texto = "  " + texto + "   "
    # Caracteres especiales adicionales
    if random.random() < 0.30:
        especiales = random.choices(["!", "?", "...", "!!!", "???", "¡", "¿"], k=random.randint(1, 3))
        texto = texto + "".join(especiales)
    return texto


def _generar_cuerpo_texto():
    plantilla = random.choice(CUERPOS)
    palabra = random.choice(PALABRAS_ES + PALABRAS_EN)
    palabra2 = random.choice(PALABRAS_ES + PALABRAS_EN)
    texto = plantilla.format(
        palabra=palabra,
        palabra2=palabra2,
        adjetivo=random.choice(ADJETIVOS),
        emoji=_emojis_random(1),
        resolucion=random.choice(RESOLUCIONES),
        num=random.randint(1, 12),
        accion=random.choice(ACCIONES),
        error_tipico=random.choice(ERRORES_TIPICOS),
        consejo=random.choice(CONSEJOS),
        queja=random.choice(QUEJAS),
        tema=random.choice(TEMAS),
        herramienta=random.choice(HERRAMIENTAS),
        tip=random.choice(TIPS),
        momento_dia=random.choice(MOMENTOS),
        excepcion=random.choice(EXCEPCIONES),
        actualizacion=random.choice(ACTUALIZACIONES),
        evento_random=random.choice(EVENTOS_RANDOM),
        nivel_urgencia=random.choice(NIVELES_URGENCIA),
        descripcion_random=random.choice(DESCRIPCIONES),
        frase_inicio=_palabras_random(2),
        frase_fin=_palabras_random(2),
        resultado=random.choice(PALABRAS_ES),
    )
    return _aplicar_ruido_texto(texto)

def _post_x():
    usuario = random.choice(USUARIOS)
    es_retweet = random.random() < 0.20
    es_reply = random.random() < 0.15

    cuerpo = _generar_cuerpo_texto()
    hashtags = _hashtags_random(random.randint(0, 4))
    emojis = _emojis_random(random.randint(0, 3))

    texto = f"{cuerpo} {hashtags} {emojis}".strip()
    texto = texto[:280]  # límite X

    menciones = []
    if random.random() < 0.30:
        menciones = [f"@{random.choice(USUARIOS)}" for _ in range(random.randint(1, 2))]

    return {
        "id": str(uuid.uuid4()),
        "red_social": "X",
        "usuario": usuario,
        "display_name": usuario.replace("_", " ").title() if random.random() > 0.4 else usuario.upper(),
        "texto": texto,
        "menciones": menciones,
        "hashtags": [h for h in hashtags.split() if h.startswith("#")],
        "likes": random.randint(0, 150_000),
        "retweets": random.randint(0, 50_000),
        "replies": random.randint(0, 5_000),
        "views": random.randint(100, 5_000_000),
        "es_retweet": es_retweet,
        "es_reply": es_reply,
        "usuario_original": random.choice(USUARIOS) if es_retweet else None,
        "idioma": random.choice(["es", "en", "es", "es", "pt", "fr"]),  # sesgo español
        "timestamp": _timestamp_random(),
        "verificado": random.random() < 0.08,
        "seguidores_cuenta": random.randint(0, 2_000_000),
        "url_media": f"https://pbs.twimg.com/media/{uuid.uuid4().hex[:11]}.jpg" if random.random() < 0.35 else None,
        "coordenadas": {
            "lat": round(random.uniform(1.0, 12.0), 6),
            "lon": round(random.uniform(-77.0, -66.0), 6),
        } if random.random() < 0.12 else None,
    }


def _post_reddit():
    usuario = random.choice(USUARIOS)
    subreddit = random.choice(SUBREDDITS)
    tipo = random.choice(["text", "text", "text", "link", "image"])

    titulo = _aplicar_ruido_texto(random.choice(TITULOS_REDDIT))

    if tipo == "text":
        parrafos = [_generar_cuerpo_texto() for _ in range(random.randint(1, 4))]
        body = "\n\n".join(parrafos)
        if random.random() < 0.40:
            body += f"\n\n**Edit:** {_generar_cuerpo_texto()}"
    elif tipo == "link":
        body = None
    else:
        body = None

    upvotes = random.randint(1, 98_000)
    downvotes = int(upvotes * random.uniform(0.02, 0.40))

    flair_options = [None, "Pregunta", "DISCUSSION", "tutorial", "rant", "Ayuda", "Solved ✅", "OC"]
    comentarios_top = [
        {
            "usuario": random.choice(USUARIOS),
            "texto": _aplicar_ruido_texto(_generar_cuerpo_texto()),
            "karma": random.randint(-50, 10_000),
            "timestamp": _timestamp_random(),
        }
        for _ in range(random.randint(0, 8))
    ]

    return {
        "id": f"t3_{uuid.uuid4().hex[:6]}",
        "red_social": "Reddit",
        "usuario": f"u/{usuario}",
        "subreddit": subreddit,
        "titulo": titulo,
        "body": body,
        "tipo_post": tipo,
        "url_externa": f"https://example.com/{uuid.uuid4().hex[:8]}" if tipo == "link" else None,
        "url_imagen": f"https://i.redd.it/{uuid.uuid4().hex[:11]}.jpg" if tipo == "image" else None,
        "upvotes": upvotes,
        "downvotes": downvotes,
        "score": upvotes - downvotes,
        "upvote_ratio": round(upvotes / (upvotes + downvotes + 1), 4),
        "num_comentarios": random.randint(0, 3_400),
        "premios": random.choices(
            [[], ["Gold"], ["Silver"], ["Gold", "Helpful"], ["Platinum", "Gold", "Silver"]],
            weights=[60, 15, 12, 8, 5],
        )[0],
        "flair": random.choice(flair_options),
        "nsfw": random.random() < 0.04,
        "spoiler": random.random() < 0.03,
        "crosspost_subreddits": random.sample(SUBREDDITS, random.randint(0, 2)),
        "comentarios_top": comentarios_top,
        "timestamp": _timestamp_random(),
        "editado": random.random() < 0.18,
        "karma_usuario": random.randint(-100, 500_000),
    }


def _post_instagram():
    usuario = random.choice(USUARIOS)

    cuerpo = _generar_cuerpo_texto()
    emojis_extra = _emojis_random(random.randint(1, 6))
    hashtags_bloque = _hashtags_random(random.randint(3, 15))  # IG usa muchos hashtags

    caption_variantes = [
        f"{cuerpo} {emojis_extra}\n.\n.\n.\n{hashtags_bloque}",
        f"{emojis_extra} {cuerpo}\n{hashtags_bloque}",
        f"{cuerpo}\n{emojis_extra} {hashtags_bloque}",
        f"{cuerpo}",
    ]
    caption = random.choice(caption_variantes)

    tipo_contenido = random.choices(
        ["foto", "carrusel", "reel", "story"],
        weights=[40, 25, 25, 10],
    )[0]

    ubicacion_opciones = [
        None, "Medellín, Colombia", "Bogotá", "Cartagena de Indias",
        "Ciudad de México", "Buenos Aires", "Madrid, España", "Miami, FL",
        "New York, NY", "Café de origen, El Poblado",
    ]

    menciones_ig = []
    if random.random() < 0.45:
        menciones_ig = [f"@{random.choice(USUARIOS)}" for _ in range(random.randint(1, 4))]

    musica = None
    if tipo_contenido in ("reel", "story") and random.random() < 0.70:
        artistas = ["Bad Bunny", "Karol G", "Shakira", "J Balvin", "Maluma", "Taylor Swift"]
        canciones = ["Tití Me Preguntó", "PROVENZA", "Hips Don't Lie", "Mi Gente", "Hawái", "Anti-Hero"]
        musica = {
            "artista": random.choice(artistas),
            "cancion": random.choice(canciones),
            "duracion_seg": random.randint(15, 90),
        }

    likes = random.randint(0, 10_000_000)
    return {
        "id": uuid.uuid4().hex,
        "red_social": "Instagram",
        "usuario": usuario,
        "nombre_completo": usuario.replace("_", " ").title(),
        "caption": caption,
        "hashtags": [h for h in hashtags_bloque.split() if h.startswith("#")],
        "menciones": menciones_ig,
        "tipo_contenido": tipo_contenido,
        "num_imagenes": random.randint(1, 10) if tipo_contenido == "carrusel" else (1 if tipo_contenido == "foto" else 0),
        "likes": likes,
        "comentarios": random.randint(0, int(likes * 0.05) + 1),
        "guardados": random.randint(0, int(likes * 0.10) + 1),
        "alcance": random.randint(likes, likes * 20 + 100),
        "impresiones": random.randint(likes, likes * 50 + 100),
        "ubicacion": random.choice(ubicacion_opciones),
        "musica": musica,
        "es_patrocinado": random.random() < 0.07,
        "cuenta_verificada": random.random() < 0.12,
        "seguidores": random.randint(100, 50_000_000),
        "siguiendo": random.randint(1, 7_500),
        "timestamp": _timestamp_random(),
        "stories_link": f"https://ig.me/s/{uuid.uuid4().hex[:8]}" if random.random() < 0.20 else None,
        "filtro_usado": random.choice([None, "Clarendon", "Gingham", "Juno", "Lark", "Moon", "Valencia"]),
    }

_GENERADORES = {
    "X":         _post_x,
    "Reddit":    _post_reddit,
    "Instagram": _post_instagram,
}

def generar_post(red_social: str | None = None) -> dict:
    if red_social is None:
        red_social = random.choice(list(_GENERADORES.keys()))

    red_social_norm = red_social.strip().capitalize()
    if red_social_norm == "Twitter":
        red_social_norm = "X"

    if red_social_norm not in _GENERADORES:
        raise ValueError(f"Red social '{red_social}' no soportada. Opciones: X, Reddit, Instagram")

    return _GENERADORES[red_social_norm]()

if __name__ == "__main__":
    import json

    print("=" * 70)
    print("DEMO: 2 posts por red social (datos sucios para Glue / ETL)")
    print("=" * 70)

    for red in ["X", "Reddit", "Instagram"]:
        print(f"\n{'─'*30} {red} {'─'*30}")
        for i in range(2):
            post = generar_post(red)
            print(f"\n[Post #{i+1}]")
            print(json.dumps(post, ensure_ascii=False, indent=2))

    print("\n" + "=" * 70)
    print("Post aleatorio (sin especificar red):")
    print("=" * 70)
    post_random = generar_post()
    print(json.dumps(post_random, ensure_ascii=False, indent=2))
