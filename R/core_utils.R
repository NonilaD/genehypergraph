# =============================================================================
# core_utils.R — Utilidades transversales del pipeline
# =============================================================================
# Cubre: formato de consola (cli, estilo B), logging, timing, filesystem,
# manejo de errores, dependencias opcionales, semilla y GC.
# =============================================================================

# =============================================================================
# 1. FORMATO DE CONSOLA — Estilo B
# =============================================================================

#' Cabecera de inicio del pipeline
#'
#' Muestra el bloque de presentacion al arrancar el pipeline, con el
#' identificador de ejecucion, traits y capas activas.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param n_traits Numero de traits a procesar.
#'
#' @return Invisible `NULL`. Se llama por sus efectos en consola.
#' @importFrom cli cli_rule cli_text col_cyan style_bold
#' @export
#'
#' @examples
#' config <- load_config()
#' gh_header(config, n_traits = 100)
gh_header <- function(config, n_traits) {
  capas_activas <- names(Filter(function(x) isTRUE(x$activar), config$capas))
  cli::cli_rule(
    left  = cli::style_bold("genehypergraph"),
    right = paste0("v", utils::packageVersion("genehypergraph"))
  )
  cli::cli_text(
    "  {cli::col_cyan('Run:')} {config$run_id}  |  ",
    "{cli::col_cyan('Traits:')} {n_traits}  |  ",
    "{cli::col_cyan('Capas:')} {paste(capas_activas, collapse = ', ')}"
  )
  cli::cli_rule()
  invisible(NULL)
}

#' Cabecera de fase del pipeline
#'
#' Imprime una linea divisoria con el nombre de la fase actual.
#'
#' @param fase Numero de fase (entero).
#' @param nombre Nombre descriptivo de la fase.
#'
#' @return Invisible `NULL`.
#' @importFrom cli cli_rule col_blue style_bold
#' @export
#'
#' @examples
#' gh_fase(1, "Preparacion de datos")
#' gh_fase(2, "Construccion de la red")
gh_fase <- function(fase, nombre) {
  cli::cli_rule(
    left = cli::style_bold(
      paste0(cli::col_blue("\u25B6"), " FASE ", fase, " \u00B7 ", toupper(nombre))
    )
  )
  invisible(NULL)
}

#' Mensaje de paso completado con tiempo
#'
#' Muestra una linea de resultado en formato tabla al estilo B:
#' `  -> Etiqueta    valor    [tiempo] simbolo`
#'
#' @param etiqueta Descripcion del paso (cadena corta).
#' @param valor Valor o resumen del resultado (p. ej. numero de genes).
#' @param tiempo Duracion en segundos (numeric). Si es `NULL` no se muestra.
#' @param ok Logico. `TRUE` muestra simbolo de exito, `FALSE` de error.
#'
#' @return Invisible `NULL`.
#' @importFrom cli cli_text col_green col_red symbol
#' @export
#'
#' @examples
#' gh_paso("Genes cargados", "19,247", tiempo = 1.2)
#' gh_paso("Traits filtrados", "844", tiempo = 0.3, ok = TRUE)
#' gh_paso("Interactoma", "error", tiempo = 0.1, ok = FALSE)
gh_paso <- function(etiqueta, valor, tiempo = NULL, ok = TRUE) {
  simbolo <- if (ok) cli::col_green(cli::symbol$tick) else cli::col_red(cli::symbol$cross)
  t_str   <- if (!is.null(tiempo)) paste0(" [", .fmt_tiempo(tiempo), "]") else ""
  cli::cli_text(
    "  {cli::col_blue('\u2192')} {etiqueta}{strrep(' ', max(1L, 30L - nchar(etiqueta)))}",
    "{valor}{t_str} {simbolo}"
  )
  invisible(NULL)
}

#' Mensaje de advertencia del pipeline
#'
#' @param msg Texto del aviso.
#'
#' @return Invisible `NULL`.
#' @importFrom cli cli_alert_warning
#' @export
#'
#' @examples
#' gh_warn("3 traits descartados por tener menos de 2 genes.")
gh_warn <- function(msg) {
  cli::cli_alert_warning(msg)
  invisible(NULL)
}

#' Resumen de fin de ejecucion
#'
#' Muestra una regla de cierre con el total de traits procesados y el tiempo
#' total de ejecucion.
#'
#' @param n_ok Numero de traits completados correctamente.
#' @param n_total Total de traits intentados.
#' @param tiempo_total Duracion total en segundos.
#'
#' @return Invisible `NULL`.
#' @importFrom cli cli_rule cli_text col_green col_red style_bold
#' @export
#'
#' @examples
#' gh_footer(n_ok = 843, n_total = 844, tiempo_total = 8040)
gh_footer <- function(n_ok, n_total, tiempo_total) {
  n_err <- n_total - n_ok
  cli::cli_rule()
  cli::cli_text(
    "  {cli::style_bold('Completado')} en {.fmt_tiempo(tiempo_total)}  |  ",
    "{cli::col_green(n_ok)}/{n_total} traits  |  ",
    "{cli::col_red(n_err)} error{if (n_err != 1) 's' else ''}"
  )
  cli::cli_rule()
  invisible(NULL)
}

# =============================================================================
# 2. BARRA DE PROGRESO
# =============================================================================

#' Crear barra de progreso por traits
#'
#' Devuelve el ID de una barra de progreso `cli` lista para actualizarse con
#' [gh_progreso_tick()] y cerrarse con [gh_progreso_done()].
#'
#' @param n_total Total de unidades (traits).
#' @param nombre Nombre de la unidad (por defecto `"trait"`).
#'
#' @return ID de la barra de progreso (entero, devuelto por `cli`).
#' @importFrom cli cli_progress_bar
#' @export
#'
#' @examples
#' \dontrun{
#' pb <- gh_progreso_init(100)
#' for (i in seq_len(100)) {
#'   Sys.sleep(0.01)
#'   gh_progreso_tick(pb, paste("trait", i))
#' }
#' gh_progreso_done(pb)
#' }
gh_progreso_init <- function(n_total, nombre = "trait") {
  cli::cli_progress_bar(
    name   = nombre,
    total  = n_total,
    format = paste0(
      "  {cli::pb_bar} {cli::pb_percent}  |  ",
      "{cli::pb_current}/{cli::pb_total} {nombre}s  |  ETA: {cli::pb_eta}"
    ),
    clear  = FALSE
  )
}

#' Avanzar la barra de progreso un paso
#'
#' @param id ID devuelto por [gh_progreso_init()].
#' @param status Texto de estado que se muestra junto a la barra (opcional).
#'
#' @return Invisible `NULL`.
#' @importFrom cli cli_progress_update
#' @export
#'
#' @examples
#' \dontrun{
#' pb <- gh_progreso_init(10)
#' gh_progreso_tick(pb, "EFO_0001645")
#' gh_progreso_done(pb)
#' }
gh_progreso_tick <- function(id, status = NULL) {
  cli::cli_progress_update(id = id, status = status)
  invisible(NULL)
}

#' Cerrar la barra de progreso
#'
#' @param id ID devuelto por [gh_progreso_init()].
#'
#' @return Invisible `NULL`.
#' @importFrom cli cli_progress_done
#' @export
#'
#' @examples
#' \dontrun{
#' pb <- gh_progreso_init(1)
#' gh_progreso_tick(pb)
#' gh_progreso_done(pb)
#' }
gh_progreso_done <- function(id) {
  cli::cli_progress_done(id = id)
  invisible(NULL)
}

# =============================================================================
# 3. LOGGING
# =============================================================================

# Niveles de log ordenados para comparacion
.LOG_NIVELES <- c(DEBUG = 1L, INFO = 2L, WARN = 3L, ERROR = 4L)

#' Emitir un mensaje de log
#'
#' Escribe un mensaje en consola y, si esta configurado, en el archivo de log.
#' El mensaje solo se emite si su nivel es >= al nivel configurado en
#' `config$logging$nivel`.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param nivel Nivel del mensaje: `"DEBUG"`, `"INFO"`, `"WARN"` o `"ERROR"`.
#' @param ... Fragmentos del mensaje, concatenados sin separador.
#'
#' @return Invisible `NULL`.
#' @importFrom cli cli_alert_info cli_alert_warning cli_alert_danger
#' @export
#'
#' @examples
#' config <- load_config()
#' gh_log(config, "INFO", "Pipeline iniciado.")
#' gh_log(config, "WARN", "Trait descartado: ", "EFO_0001645")
gh_log <- function(config, nivel, ...) {
  nivel_config <- toupper(config$logging$nivel)
  if (.LOG_NIVELES[[nivel]] < .LOG_NIVELES[[nivel_config]]) return(invisible(NULL))

  msg       <- paste0(...)
  timestamp <- format(Sys.time(), "[%H:%M:%S]")
  linea     <- paste(timestamp, nivel, msg)

  # Consola
  switch(nivel,
    DEBUG = message(linea),
    INFO  = cli::cli_alert_info(msg),
    WARN  = cli::cli_alert_warning(msg),
    ERROR = cli::cli_alert_danger(msg)
  )

  # Archivo
  if (isTRUE(config$logging$guardar_archivo)) {
    log_path <- file.path(
      get_config(config, "rutas.outputs.logs"),
      config$logging$archivo
    )
    if (!is.null(log_path) && nzchar(log_path)) {
      cat(linea, "\n", file = log_path, append = TRUE)
    }
  }

  invisible(NULL)
}

# =============================================================================
# 4. TIMING
# =============================================================================

#' Iniciar un cronometro
#'
#' Devuelve el tiempo de inicio para usarlo con [gh_toc()].
#'
#' @return Objeto `proc_time` con el tiempo actual.
#' @export
#'
#' @examples
#' t0 <- gh_tic()
#' Sys.sleep(0.1)
#' gh_toc(t0)
gh_tic <- function() {
  proc.time()
}

#' Calcular tiempo transcurrido
#'
#' @param t0 Objeto `proc_time` devuelto por [gh_tic()].
#'
#' @return Tiempo transcurrido en segundos (numeric).
#' @export
#'
#' @examples
#' t0 <- gh_tic()
#' elapsed <- gh_toc(t0)
gh_toc <- function(t0) {
  as.numeric((proc.time() - t0)[["elapsed"]])
}

# Formateador interno de segundos -> string legible
.fmt_tiempo <- function(segundos) {
  if (segundos < 60) {
    paste0(round(segundos, 1), "s")
  } else if (segundos < 3600) {
    paste0(floor(segundos / 60), "m ", round(segundos %% 60), "s")
  } else {
    h <- floor(segundos / 3600)
    m <- floor((segundos %% 3600) / 60)
    paste0(h, "h ", m, "m")
  }
}

# =============================================================================
# 5. FILESYSTEM
# =============================================================================

#' Crear directorio si no existe
#'
#' Crea la ruta completa (equivalente a `mkdir -p`) sin error si ya existe.
#'
#' @param path Ruta del directorio a crear.
#'
#' @return Invisible `path`.
#' @export
#'
#' @examples
#' \dontrun{
#' ensure_dir(file.path(tempdir(), "genehypergraph", "outputs"))
#' }
ensure_dir <- function(path) {
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }
  invisible(path)
}

#' Construir ruta de output para un trait y capa
#'
#' Combina la ruta base de outputs del config con los placeholders de trait y
#' capa, y crea el directorio si no existe.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param tipo Clave de ruta en `rutas.outputs`, p. ej. `"visualizacion"` o
#'   `"traits"`.
#' @param trait_id ID EFO/MONDO del trait.
#' @param trait_name Nombre legible del trait.
#' @param capa Nombre de la capa. Si es `NULL` no se resuelve `{capa}`.
#' @param crear Logico. Si `TRUE` (por defecto) crea el directorio.
#'
#' @return Ruta resuelta como cadena de texto.
#' @export
#'
#' @examples
#' config <- load_config()
#' \dontrun{
#' build_output_path(config, "traits", "EFO_0001645", "alzheimer")
#' build_output_path(config, "visualizacion", "EFO_0001645", "alzheimer", "gwas_comun")
#' }
build_output_path <- function(config, tipo, trait_id, trait_name,
                               capa = NULL, crear = TRUE) {
  base <- get_config(config, paste0("rutas.outputs.", tipo))
  if (is.null(base)) stop("Ruta no encontrada en config: rutas.outputs.", tipo)
  path <- resolve_trait_path(base, trait_id, trait_name, capa)
  if (crear) ensure_dir(path)
  path
}

# =============================================================================
# 6. MANEJO DE ERRORES
# =============================================================================

#' Ejecutar un paso del pipeline con politica de errores
#'
#' Envuelve la ejecucion de una funcion aplicando la politica definida en
#' `config$errores$politica`:
#' - `"parar"`: propaga el error (comportamiento por defecto de R).
#' - `"continuar_trait"`: captura el error, lo registra y devuelve `NULL`.
#' - `"continuar_run"`: igual que `continuar_trait`.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param paso Nombre descriptivo del paso (para el mensaje de error).
#' @param expr Expresion a evaluar (se pasa sin comillas, como en
#'   `tryCatch`).
#'
#' @return El resultado de `expr`, o `NULL` si se capturo un error y la
#'   politica no es `"parar"`.
#' @export
#'
#' @examples
#' config <- load_config()
#' result <- try_step(config, "carga de genes", {
#'   1 + 1
#' })
try_step <- function(config, paso, expr) {
  politica <- get_config(config, "errores.politica", default = "parar")
  if (politica == "parar") {
    return(expr)
  }
  tryCatch(
    expr,
    error = function(e) {
      gh_log(config, "ERROR", "Fallo en '", paso, "': ", conditionMessage(e))
      NULL
    }
  )
}

# =============================================================================
# 7. DEPENDENCIAS OPCIONALES (Suggests)
# =============================================================================

#' Comprobar si un paquete de Suggests esta disponible
#'
#' Emite un mensaje claro si el paquete no esta instalado, en lugar de un
#' error crипtico. Util para modulos que dependen de Bioconductor o paquetes
#' opcionales.
#'
#' @param pkg Nombre del paquete.
#' @param modulo Nombre del modulo que lo necesita (para el mensaje).
#'
#' @return `TRUE` si el paquete esta disponible, `FALSE` en caso contrario
#'   (con un `message()` informativo).
#' @export
#'
#' @examples
#' check_suggests("cli", "core_utils")
#' check_suggests("paqueteInexistente", "modulo_ejemplo")
check_suggests <- function(pkg, modulo) {
  if (requireNamespace(pkg, quietly = TRUE)) return(TRUE)
  message(
    "El paquete '", pkg, "' es necesario para '", modulo, "' pero no esta ",
    "instalado.\n  Instalalo con: install.packages('", pkg, "')"
  )
  FALSE
}

# =============================================================================
# 8. SEMILLA Y RECURSOS
# =============================================================================

#' Fijar la semilla aleatoria desde la configuracion
#'
#' Aplica `set.seed()` con el valor de `config$ejecucion$semilla` para
#' garantizar reproducibilidad en permutaciones, UMAP, clustering, etc.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#'
#' @return Invisible `NULL`.
#' @export
#'
#' @examples
#' config <- load_config()
#' gh_set_seed(config)
gh_set_seed <- function(config) {
  set.seed(config$ejecucion$semilla)
  invisible(NULL)
}

#' Ejecutar recoleccion de basura entre pasos si esta configurado
#'
#' Llama a `gc()` solo si `config$rendimiento$gc_entre_pasos` es `TRUE`.
#' Reduce el uso de RAM entre pasos pesados del pipeline.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#'
#' @return Invisible `NULL`.
#' @export
#'
#' @examples
#' config <- load_config()
#' gh_gc(config)
gh_gc <- function(config) {
  if (isTRUE(config$rendimiento$gc_entre_pasos)) gc(verbose = FALSE)
  invisible(NULL)
}

# =============================================================================
# 9. MANIFEST
# =============================================================================

#' Registrar una entrada en el manifest de la ejecucion
#'
#' Añade una linea al manifest YAML de la ejecucion actual con el nombre del
#' paso, estado, tiempo y mensaje opcional. El manifest se guarda en
#' `outputs/{run_id}/global/parametros/manifest.yaml`.
#'
#' @param config Lista de configuracion devuelta por [load_config()].
#' @param paso Nombre del paso registrado.
#' @param estado `"ok"`, `"error"` o `"omitido"`.
#' @param tiempo Duracion en segundos (numeric). Puede ser `NULL`.
#' @param msg Mensaje adicional opcional.
#'
#' @return Invisible `NULL`.
#' @importFrom yaml write_yaml
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' gh_manifest(config, "network_genes", "ok", tiempo = 1.2)
#' gh_manifest(config, "analysis_latent", "omitido")
#' }
gh_manifest <- function(config, paso, estado, tiempo = NULL, msg = NULL) {
  global_dir  <- get_config(config, "rutas.outputs.global")
  if (is.null(global_dir)) return(invisible(NULL))

  manifest_dir  <- file.path(global_dir, "parametros")
  manifest_path <- file.path(manifest_dir, "manifest.yaml")
  ensure_dir(manifest_dir)

  entrada <- list(
    paso      = paso,
    estado    = estado,
    timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"),
    tiempo_s  = tiempo,
    mensaje   = msg
  )

  # Leer manifest existente y añadir entrada
  actual <- if (file.exists(manifest_path)) {
    yaml::read_yaml(manifest_path)
  } else {
    list()
  }
  actual[[length(actual) + 1L]] <- entrada
  yaml::write_yaml(actual, manifest_path)

  invisible(NULL)
}
