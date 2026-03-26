# =============================================================================
# core_cache.R — Sistema de cache del pipeline
# =============================================================================
# Backend: qs2 (rapido, comprimido) con fallback automatico a rds (base R).
# Modo hash:      la clave es el MD5 de los inputs -> cambio de input = miss.
# Modo timestamp: la clave es un nombre fijo     -> hit si el archivo existe.
# =============================================================================

# =============================================================================
# Internos
# =============================================================================

# Construir la ruta de un archivo de cache a partir de su clave
.cache_path <- function(config, key) {
  dir    <- get_config(config, "rutas.outputs.cache")
  fmt    <- get_config(config, "cache.formato", default = "rds")
  ext    <- if (fmt == "qs2") "qs2" else "rds"
  file.path(dir, paste0(key, ".", ext))
}

# Leer un objeto desde cache (qs2 o rds)
.cache_read <- function(path, formato) {
  if (formato == "qs2") {
    qs2::qread(path)
  } else {
    readRDS(path)
  }
}

# Escribir un objeto a cache (qs2 o rds)
.cache_write <- function(object, path, formato) {
  ensure_dir(dirname(path))
  if (formato == "qs2") {
    qs2::qsave(object, path)
  } else {
    saveRDS(object, path)
  }
  invisible(path)
}

# Resolver el formato efectivo: qs2 si esta disponible, rds si no
.cache_formato <- function(config) {
  fmt <- get_config(config, "cache.formato", default = "rds")
  if (fmt == "qs2" && !requireNamespace("qs2", quietly = TRUE)) {
    message(
      "El paquete 'qs2' no esta instalado. Usando 'rds' como backend de cache.\n",
      "  Instalalo con: install.packages('qs2')"
    )
    return("rds")
  }
  fmt
}

# =============================================================================
# API publica
# =============================================================================

#' Generar una clave de cache a partir de objetos R
#'
#' Serializa los objetos recibidos y calcula su huella MD5 usando
#' `tools::md5sum()`. La clave cambia si cambia cualquier input, lo que
#' garantiza invalidacion automatica en modo `"hash"`.
#'
#' @param ... Objetos R que determinan la clave (datos, parametros, config...).
#'
#' @return Cadena hexadecimal de 32 caracteres (MD5).
#' @export
#'
#' @examples
#' cache_key(mtcars, list(umbral = 0.75))
#' cache_key("network_genes", list(biotype = "protein_coding"))
cache_key <- function(...) {
  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  raw_bytes <- serialize(list(...), connection = NULL)
  writeBin(raw_bytes, tmp)
  unname(tools::md5sum(tmp))
}

#' Comprobar si existe una entrada valida en cache
#'
#' Devuelve `TRUE` si el archivo de cache correspondiente a `key` existe en
#' disco. No valida el contenido del archivo.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param key Clave de cache, generada con [cache_key()] o cadena libre.
#'
#' @return Logico.
#' @export
#'
#' @examples
#' config <- load_config()
#' key <- cache_key("ejemplo", 42L)
#' cache_exists(config, key)
cache_exists <- function(config, key) {
  if (!isTRUE(get_config(config, "cache.activar"))) return(FALSE)
  file.exists(.cache_path(config, key))
}

#' Recuperar un objeto desde cache
#'
#' Lee y devuelve el objeto almacenado bajo `key`. Devuelve `NULL` si la
#' entrada no existe o si el cache esta desactivado.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param key Clave de cache.
#'
#' @return El objeto cacheado, o `NULL` si no existe.
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' key    <- cache_key("network_genes", config$filtros$genes)
#' objeto <- cache_get(config, key)
#' }
cache_get <- function(config, key) {
  if (!cache_exists(config, key)) return(NULL)
  fmt  <- .cache_formato(config)
  path <- .cache_path(config, key)
  .cache_read(path, fmt)
}

#' Guardar un objeto en cache
#'
#' Almacena `object` en disco bajo la clave `key`. Si el cache esta
#' desactivado en config, la llamada no tiene efecto.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param key Clave de cache.
#' @param object Objeto R a almacenar.
#'
#' @return Invisible `NULL`.
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' key    <- cache_key("network_genes", config$filtros$genes)
#' cache_set(config, key, resultado)
#' }
cache_set <- function(config, key, object) {
  if (!isTRUE(get_config(config, "cache.activar"))) return(invisible(NULL))
  fmt  <- .cache_formato(config)
  path <- .cache_path(config, key)
  .cache_write(object, path, fmt)
  invisible(NULL)
}

#' Obtener desde cache o calcular y almacenar
#'
#' Patron principal de uso del cache en el pipeline. Si existe una entrada
#' valida para `key`, la devuelve directamente. Si no, evalua `expr`, guarda
#' el resultado y lo devuelve.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param key Clave de cache generada con [cache_key()].
#' @param expr Expresion a evaluar en caso de cache miss (sin comillas).
#'
#' @return El resultado de `expr` (desde cache o recien calculado).
#' @export
#'
#' @examples
#' config <- load_config()
#' key <- cache_key("suma_ejemplo", 1L, 2L)
#' resultado <- cache_or_compute(config, key, {
#'   1L + 2L
#' })
cache_or_compute <- function(config, key, expr) {
  cached <- cache_get(config, key)
  if (!is.null(cached)) return(cached)
  result <- expr
  cache_set(config, key, result)
  result
}

#' Eliminar una entrada del cache
#'
#' Borra el archivo de cache correspondiente a `key` si existe.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param key Clave de cache.
#'
#' @return Invisible logico: `TRUE` si se elimino, `FALSE` si no existia.
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' key <- cache_key("network_genes", config$filtros$genes)
#' cache_invalidate(config, key)
#' }
cache_invalidate <- function(config, key) {
  path <- .cache_path(config, key)
  if (file.exists(path)) {
    unlink(path)
    return(invisible(TRUE))
  }
  invisible(FALSE)
}

#' Limpiar todo el cache de la ejecucion
#'
#' Elimina todos los archivos de la carpeta de cache definida en
#' `rutas.outputs.cache`. No elimina la carpeta en si.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#'
#' @return Invisible numero de archivos eliminados.
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' cache_clear(config)
#' }
cache_clear <- function(config) {
  dir <- get_config(config, "rutas.outputs.cache")
  if (is.null(dir) || !dir.exists(dir)) return(invisible(0L))
  archivos <- list.files(dir, full.names = TRUE,
                         pattern = "\\.(rds|qs2)$")
  unlink(archivos)
  invisible(length(archivos))
}
