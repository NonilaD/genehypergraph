# =============================================================================
# core_cache.R — Pipeline cache system
# =============================================================================
# Backend: qs2 (fast, compressed) with automatic fallback to rds (base R).
# Hash mode:      the key is the MD5 of the inputs -> input change = miss.
# Timestamp mode: the key is a fixed name          -> hit if the file exists.
#
# Cache directory:
#   Uses rutas.cache_global (new top-level key in config) as a fixed folder
#   shared across all pipeline runs.  Falls back to
#   file.path(dirname(rutas.outputs.cache), "cache_global") when the key is
#   absent — i.e., "outputs/cache_global" with default YAML values.
#   This ensures cache files survive across run_id rotations.
# =============================================================================

# =============================================================================
# Internals
# =============================================================================

# Resolve the shared cache directory (independent of run_id)
.cache_dir <- function(config) {
  dir <- get_config(config, "rutas.cache_global", default = NULL)
  if (!is.null(dir)) return(dir)
  base <- get_config(config, "rutas.outputs.cache", default = "outputs/cache")
  file.path(dirname(base), "cache_global")
}

# Build the full path of a cache file from its key
.cache_path <- function(config, key) {
  dir <- .cache_dir(config)
  fmt <- get_config(config, "cache.formato", default = "rds")
  ext <- if (fmt == "qs2") "qs2" else "rds"
  file.path(dir, paste0(key, ".", ext))
}

# Returns TRUE if the cache file exists but has expired (max_age_days check).
# When cache.max_age_days is absent or NULL the cache never expires.
.cache_expired <- function(path, config) {
  max_age <- get_config(config, "cache.max_age_days", default = NULL)
  if (is.null(max_age)) return(FALSE)
  file.exists(path) && (file.mtime(path) < Sys.time() - max_age * 86400)
}

# Read an object from cache (qs2 or rds)
.cache_read <- function(path, formato) {
  if (formato == "qs2") {
    qs2::qs_read(path)
  } else {
    readRDS(path)
  }
}

# Write an object to cache (qs2 or rds)
.cache_write <- function(object, path, formato) {
  ensure_dir(dirname(path))
  if (formato == "qs2") {
    qs2::qs_save(object, path)
  } else {
    saveRDS(object, path)
  }
  invisible(path)
}

# Resolve the effective format: qs2 if available, rds otherwise
.cache_format <- function(config) {
  fmt <- get_config(config, "cache.formato", default = "rds")
  if (fmt == "qs2" && !requireNamespace("qs2", quietly = TRUE)) {
    message(
      "Package 'qs2' is not installed. Using 'rds' as cache backend.\n",
      "  Install it with: install.packages('qs2')"
    )
    return("rds")
  }
  fmt
}

# =============================================================================
# Public API
# =============================================================================

#' Generate a cache key from R objects
#'
#' \[EN\] Serialises the received objects and computes their MD5 fingerprint
#' using `tools::md5sum()`. The key changes if any input changes, which
#' guarantees automatic invalidation in `"hash"` mode.
#'
#' \[ESP\] Serializa los objetos recibidos y calcula su huella MD5 usando
#' `tools::md5sum()`. La clave cambia si cambia cualquier input, lo que
#' garantiza invalidacion automatica en modo `"hash"`.
#'
#' @param ... \[EN\] R objects that determine the key (data, parameters,
#'   config...).\cr
#'   \[ESP\] Objetos R que determinan la clave (datos, parametros, config...).
#'
#' @return \[EN\] 32-character hexadecimal string (MD5).\cr
#'   \[ESP\] Cadena hexadecimal de 32 caracteres (MD5).
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

#' Check if a valid cache entry exists
#'
#' \[EN\] Returns `TRUE` if the cache file corresponding to `key` exists on
#' disk **and** has not expired. A file is considered expired when
#' `cache.max_age_days` is set in config and the file's modification time is
#' older than that many days. When `cache.max_age_days` is absent the cache
#' never expires. Returns `FALSE` if the cache is disabled.
#'
#' \[ESP\] Devuelve `TRUE` si el archivo de cache correspondiente a `key`
#' existe en disco **y** no ha expirado. Un archivo se considera expirado
#' cuando `cache.max_age_days` esta definido en config y la fecha de
#' modificacion del archivo es anterior a ese numero de dias. Si
#' `cache.max_age_days` no existe, el cache nunca expira. Devuelve `FALSE` si
#' el cache esta desactivado.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param key \[EN\] Cache key, generated with [cache_key()] or a free
#'   string.\cr
#'   \[ESP\] Clave de cache, generada con [cache_key()] o cadena libre.
#'
#' @return \[EN\] Logical.\cr
#'   \[ESP\] Logico.
#' @export
#'
#' @examples
#' config <- load_config()
#' key <- cache_key("ejemplo", 42L)
#' cache_exists(config, key)
cache_exists <- function(config, key) {
  if (!isTRUE(get_config(config, "cache.activar"))) return(FALSE)
  path <- .cache_path(config, key)
  if (!file.exists(path)) return(FALSE)
  if (.cache_expired(path, config)) return(FALSE)
  TRUE
}

#' Retrieve an object from cache
#'
#' \[EN\] Reads and returns the object stored under `key`. Returns `NULL` if
#' the entry does not exist, has expired, or the cache is disabled. Emits a
#' `DEBUG` log message on hit and on miss.
#'
#' \[ESP\] Lee y devuelve el objeto almacenado bajo `key`. Devuelve `NULL` si
#' la entrada no existe, ha expirado o el cache esta desactivado. Emite un
#' mensaje de log `DEBUG` en caso de hit y de miss.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param key \[EN\] Cache key.\cr
#'   \[ESP\] Clave de cache.
#'
#' @return \[EN\] The cached object, or `NULL` if it does not exist.\cr
#'   \[ESP\] El objeto cacheado, o `NULL` si no existe.
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' key    <- cache_key("network_genes", config$filtros$genes)
#' objeto <- cache_get(config, key)
#' }
cache_get <- function(config, key) {
  if (!cache_exists(config, key)) {
    gh_log(config, "DEBUG", "Cache miss: ", key)
    return(NULL)
  }
  gh_log(config, "DEBUG", "Cache hit: ", key)
  fmt  <- .cache_format(config)
  path <- .cache_path(config, key)
  .cache_read(path, fmt)
}

#' Save an object to cache
#'
#' \[EN\] Stores `object` on disk under `key`. If the cache is disabled in
#' config, the call has no effect. Emits a `DEBUG` log message after writing.
#'
#' \[ESP\] Almacena `object` en disco bajo la clave `key`. Si el cache esta
#' desactivado en config, la llamada no tiene efecto. Emite un mensaje de log
#' `DEBUG` tras la escritura.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param key \[EN\] Cache key.\cr
#'   \[ESP\] Clave de cache.
#' @param object \[EN\] R object to store.\cr
#'   \[ESP\] Objeto R a almacenar.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
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
  fmt  <- .cache_format(config)
  path <- .cache_path(config, key)
  .cache_write(object, path, fmt)
  gh_log(config, "DEBUG", "Cache written: ", key)
  invisible(NULL)
}

#' Get from cache or compute and store
#'
#' \[EN\] Main cache usage pattern in the pipeline. If a valid entry exists for
#' `key`, returns it directly. Otherwise evaluates `expr`, stores the result
#' and returns it.
#'
#' \[ESP\] Patron principal de uso del cache en el pipeline. Si existe una
#' entrada valida para `key`, la devuelve directamente. Si no, evalua `expr`,
#' guarda el resultado y lo devuelve.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param key \[EN\] Cache key generated with [cache_key()].\cr
#'   \[ESP\] Clave de cache generada con [cache_key()].
#' @param expr \[EN\] Expression to evaluate on cache miss (unquoted).\cr
#'   \[ESP\] Expresion a evaluar en caso de cache miss (sin comillas).
#'
#' @return \[EN\] The result of `expr` (from cache or freshly computed).\cr
#'   \[ESP\] El resultado de `expr` (desde cache o recien calculado).
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' key <- cache_key("suma_ejemplo", 1L, 2L)
#' resultado <- cache_or_compute(config, key, {
#'   1L + 2L
#' })
#' }
cache_or_compute <- function(config, key, expr) {
  cached <- cache_get(config, key)
  if (!is.null(cached)) return(cached)
  result <- expr
  cache_set(config, key, result)
  result
}

#' Invalidate a cache entry
#'
#' \[EN\] Deletes the cache file corresponding to `key` if it exists.
#'
#' \[ESP\] Borra el archivo de cache correspondiente a `key` si existe.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param key \[EN\] Cache key.\cr
#'   \[ESP\] Clave de cache.
#'
#' @return \[EN\] Invisible logical: `TRUE` if deleted, `FALSE` if it did not
#'   exist.\cr
#'   \[ESP\] Invisible logico: `TRUE` si se elimino, `FALSE` si no existia.
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

#' Clear the entire shared cache
#'
#' \[EN\] Deletes all `.rds` and `.qs2` files in the shared cache directory
#' (resolved via `rutas.cache_global`, or the fallback path derived from
#' `rutas.outputs.cache`). Does not delete the directory itself.
#'
#' \[ESP\] Elimina todos los archivos `.rds` y `.qs2` del directorio de cache
#' compartido (resuelto via `rutas.cache_global`, o la ruta de fallback
#' derivada de `rutas.outputs.cache`). No elimina el directorio en si.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#'
#' @return \[EN\] Invisible number of files deleted.\cr
#'   \[ESP\] Invisible numero de archivos eliminados.
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' cache_clear(config)
#' }
cache_clear <- function(config) {
  dir <- .cache_dir(config)
  if (is.null(dir) || !dir.exists(dir)) return(invisible(0L))
  files <- list.files(dir, full.names = TRUE, pattern = "\\.(rds|qs2)$")
  unlink(files)
  invisible(length(files))
}
