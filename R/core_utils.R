# =============================================================================
# core_utils.R — Cross-cutting pipeline utilities
# =============================================================================
# Covers: console formatting (cli, style B), logging, timing, filesystem,
# error handling, optional dependencies, seed and GC.
# =============================================================================

# =============================================================================
# 1. CONSOLE FORMATTING — Style B
# =============================================================================

#' Pipeline start header
#'
#' \[EN\] Displays the presentation block when the pipeline starts, with the
#' execution identifier, traits and active layers.
#'
#' \[ESP\] Muestra el bloque de presentacion al arrancar el pipeline, con el
#' identificador de ejecucion, traits y capas activas.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param n_traits \[EN\] Number of traits to process.\cr
#'   \[ESP\] Numero de traits a procesar.
#'
#' @return \[EN\] Invisible `NULL`. Called for its console side effects.\cr
#'   \[ESP\] Invisible `NULL`. Se llama por sus efectos en consola.
#' @importFrom cli cli_rule cli_text col_cyan style_bold
#' @export
#'
#' @examples
#' config <- load_config()
#' gh_header(config, n_traits = 100)
gh_header <- function(config, n_traits) {
  active_layers <- names(Filter(function(x) isTRUE(x$activar), config$capas))
  cli::cli_rule(
    left  = cli::style_bold("genehypergraph"),
    right = paste0("v", utils::packageVersion("genehypergraph"))
  )
  cli::cli_text(
    "  {cli::col_cyan('Run:')} {config$run_id}  |  ",
    "{cli::col_cyan('Traits:')} {n_traits}  |  ",
    "{cli::col_cyan('Layers:')} {paste(active_layers, collapse = ', ')}"
  )
  cli::cli_rule()
  invisible(NULL)
}

#' Pipeline phase header
#'
#' \[EN\] Prints a divider line with the name of the current phase.
#'
#' \[ESP\] Imprime una linea divisoria con el nombre de la fase actual.
#'
#' @param phase \[EN\] Phase number (integer).\cr
#'   \[ESP\] Numero de fase (entero).
#' @param name \[EN\] Descriptive name of the phase.\cr
#'   \[ESP\] Nombre descriptivo de la fase.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_rule col_blue style_bold
#' @export
#'
#' @examples
#' gh_phase(1, "Data preparation")
#' gh_phase(2, "Network construction")
gh_phase <- function(phase, name) {
  cli::cli_rule(
    left = cli::style_bold(
      paste0(cli::col_blue("\u25B6"), " PHASE ", phase, " \u00B7 ", toupper(name))
    )
  )
  invisible(NULL)
}

#' Completed step message with elapsed time
#'
#' \[EN\] Displays a result line in style B table format:
#' `  -> Label    value    [time] symbol`
#'
#' \[ESP\] Muestra una linea de resultado en formato tabla al estilo B:
#' `  -> Etiqueta    valor    [tiempo] simbolo`
#'
#' @param label \[EN\] Step description (short string).\cr
#'   \[ESP\] Descripcion del paso (cadena corta).
#' @param value \[EN\] Value or result summary (e.g. number of genes).\cr
#'   \[ESP\] Valor o resumen del resultado (p. ej. numero de genes).
#' @param time \[EN\] Duration in seconds (numeric). If `NULL` it is not
#'   shown.\cr
#'   \[ESP\] Duracion en segundos (numeric). Si es `NULL` no se muestra.
#' @param ok \[EN\] Logical. `TRUE` shows a success symbol, `FALSE` an error
#'   symbol.\cr
#'   \[ESP\] Logico. `TRUE` muestra simbolo de exito, `FALSE` de error.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_text col_green col_red symbol
#' @export
#'
#' @examples
#' gh_step("Genes loaded", "19,247", time = 1.2)
#' gh_step("Traits filtered", "844", time = 0.3, ok = TRUE)
#' gh_step("Interactome", "error", time = 0.1, ok = FALSE)
gh_step <- function(label, value, time = NULL, ok = TRUE) {
  sym   <- if (ok) cli::col_green(cli::symbol$tick) else cli::col_red(cli::symbol$cross)
  t_str <- if (!is.null(time)) paste0(" [", .fmt_time(time), "]") else ""
  cli::cli_text(
    "  {cli::col_blue('\u2192')} {label}{strrep(' ', max(1L, 30L - nchar(label)))}",
    "{value}{t_str} {sym}"
  )
  invisible(NULL)
}

#' Pipeline warning message
#'
#' \[EN\] Emits a warning alert via cli.
#'
#' \[ESP\] Emite una alerta de aviso via cli.
#'
#' @param msg \[EN\] Warning text.\cr
#'   \[ESP\] Texto del aviso.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_alert_warning
#' @export
#'
#' @examples
#' gh_warn("3 traits discarded for having fewer than 2 genes.")
gh_warn <- function(msg) {
  cli::cli_alert_warning(msg)
  invisible(NULL)
}

#' Execution end summary
#'
#' \[EN\] Displays a closing rule with the total traits processed and total
#' execution time.
#'
#' \[ESP\] Muestra una regla de cierre con el total de traits procesados y el
#' tiempo total de ejecucion.
#'
#' @param n_ok \[EN\] Number of traits completed successfully.\cr
#'   \[ESP\] Numero de traits completados correctamente.
#' @param n_total \[EN\] Total traits attempted.\cr
#'   \[ESP\] Total de traits intentados.
#' @param tiempo_total \[EN\] Total duration in seconds.\cr
#'   \[ESP\] Duracion total en segundos.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_rule cli_text col_green col_red style_bold
#' @export
#'
#' @examples
#' gh_footer(n_ok = 843, n_total = 844, tiempo_total = 8040)
gh_footer <- function(n_ok, n_total, tiempo_total) {
  n_err <- n_total - n_ok
  t_str <- .fmt_time(tiempo_total)
  cli::cli_rule()
  cli::cli_text(
    "  {cli::style_bold('Completed')} in {t_str}  |  ",
    "{cli::col_green(n_ok)}/{n_total} traits  |  ",
    "{cli::col_red(n_err)} error{if (n_err != 1) 's' else ''}"
  )
  cli::cli_rule()
  invisible(NULL)
}

# =============================================================================
# 2. PROGRESS BAR
# =============================================================================

#' Create a trait progress bar
#'
#' \[EN\] Returns the ID of a `cli` progress bar ready to be updated with
#' [gh_progress_tick()] and closed with [gh_progress_done()].
#'
#' \[ESP\] Devuelve el ID de una barra de progreso `cli` lista para
#' actualizarse con [gh_progress_tick()] y cerrarse con [gh_progress_done()].
#'
#' @param n_total \[EN\] Total number of units (traits).\cr
#'   \[ESP\] Total de unidades (traits).
#' @param name \[EN\] Name of the unit (default `"trait"`).\cr
#'   \[ESP\] Nombre de la unidad (por defecto `"trait"`).
#'
#' @return \[EN\] Progress bar ID (integer, returned by `cli`).\cr
#'   \[ESP\] ID de la barra de progreso (entero, devuelto por `cli`).
#' @importFrom cli cli_progress_bar
#' @export
#'
#' @examples
#' \dontrun{
#' pb <- gh_progress_init(100)
#' for (i in seq_len(100)) {
#'   Sys.sleep(0.01)
#'   gh_progress_tick(pb, paste("trait", i))
#' }
#' gh_progress_done(pb)
#' }
gh_progress_init <- function(n_total, name = "trait") {
  cli::cli_progress_bar(
    name   = name,
    total  = n_total,
    format = paste0(
      "  {cli::pb_bar} {cli::pb_percent}  |  ",
      "{cli::pb_current}/{cli::pb_total} {name}s  |  ETA: {cli::pb_eta}"
    ),
    clear  = FALSE
  )
}

#' Advance the progress bar one step
#'
#' \[EN\] Advances the progress bar by one tick and optionally shows a status
#' message.
#'
#' \[ESP\] Avanza la barra de progreso un paso y, opcionalmente, muestra un
#' mensaje de estado.
#'
#' @param id \[EN\] ID returned by [gh_progress_init()].\cr
#'   \[ESP\] ID devuelto por [gh_progress_init()].
#' @param status \[EN\] Status text shown next to the bar (optional).\cr
#'   \[ESP\] Texto de estado que se muestra junto a la barra (opcional).
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_progress_update
#' @export
#'
#' @examples
#' \dontrun{
#' pb <- gh_progress_init(10)
#' gh_progress_tick(pb, "EFO_0001645")
#' gh_progress_done(pb)
#' }
gh_progress_tick <- function(id, status = NULL) {
  cli::cli_progress_update(id = id, status = status)
  invisible(NULL)
}

#' Close the progress bar
#'
#' \[EN\] Closes and finalises the progress bar.
#'
#' \[ESP\] Cierra y finaliza la barra de progreso.
#'
#' @param id \[EN\] ID returned by [gh_progress_init()].\cr
#'   \[ESP\] ID devuelto por [gh_progress_init()].
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_progress_done
#' @export
#'
#' @examples
#' \dontrun{
#' pb <- gh_progress_init(1)
#' gh_progress_tick(pb)
#' gh_progress_done(pb)
#' }
gh_progress_done <- function(id) {
  cli::cli_progress_done(id = id)
  invisible(NULL)
}

# =============================================================================
# 3. LOGGING
# =============================================================================

# Log levels ordered for comparison
.LOG_LEVELS <- c(DEBUG = 1L, INFO = 2L, WARN = 3L, ERROR = 4L)

#' Emit a log message
#'
#' \[EN\] Writes a message to the console and, if configured, to the log file.
#' The message is only emitted if its level is >= the level configured in
#' `config$logging$nivel`.
#'
#' \[ESP\] Escribe un mensaje en consola y, si esta configurado, en el archivo
#' de log. El mensaje solo se emite si su nivel es >= al nivel configurado en
#' `config$logging$nivel`.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param level \[EN\] Message level: `"DEBUG"`, `"INFO"`, `"WARN"` or
#'   `"ERROR"`.\cr
#'   \[ESP\] Nivel del mensaje: `"DEBUG"`, `"INFO"`, `"WARN"` o `"ERROR"`.
#' @param ... \[EN\] Message fragments, concatenated without separator.\cr
#'   \[ESP\] Fragmentos del mensaje, concatenados sin separador.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_alert_info cli_alert_warning cli_alert_danger
#' @export
#'
#' @examples
#' config <- load_config()
#' gh_log(config, "INFO", "Pipeline started.")
#' gh_log(config, "WARN", "Trait discarded: ", "EFO_0001645")
gh_log <- function(config, level, ...) {
  config_level <- toupper(config$logging$nivel)
  if (.LOG_LEVELS[[level]] < .LOG_LEVELS[[config_level]]) return(invisible(NULL))

  msg       <- paste0(...)
  timestamp <- format(Sys.time(), "[%H:%M:%S]")
  line      <- paste(timestamp, level, msg)

  # Console output
  switch(level,
    DEBUG = message(line),
    INFO  = cli::cli_alert_info(msg),
    WARN  = cli::cli_alert_warning(msg),
    ERROR = cli::cli_alert_danger(msg)
  )

  # File output
  if (isTRUE(config$logging$guardar_archivo)) {
    log_dir  <- get_config(config, "rutas.outputs.logs")
    log_path <- file.path(log_dir, config$logging$archivo)
    if (!is.null(log_dir) && nzchar(log_dir) && dir.exists(log_dir)) {
      cat(line, "\n", file = log_path, append = TRUE)
    }
  }

  invisible(NULL)
}

# =============================================================================
# 4. TIMING
# =============================================================================

#' Start a stopwatch
#'
#' \[EN\] Returns the start time for use with [gh_toc()].
#'
#' \[ESP\] Devuelve el tiempo de inicio para usarlo con [gh_toc()].
#'
#' @return \[EN\] `proc_time` object with the current time.\cr
#'   \[ESP\] Objeto `proc_time` con el tiempo actual.
#' @export
#'
#' @examples
#' t0 <- gh_tic()
#' Sys.sleep(0.1)
#' gh_toc(t0)
gh_tic <- function() {
  proc.time()
}

#' Calculate elapsed time
#'
#' \[EN\] Computes the elapsed time since the stopwatch was started.
#'
#' \[ESP\] Calcula el tiempo transcurrido desde que se inicio el cronometro.
#'
#' @param t0 \[EN\] `proc_time` object returned by [gh_tic()].\cr
#'   \[ESP\] Objeto `proc_time` devuelto por [gh_tic()].
#'
#' @return \[EN\] Elapsed time in seconds (numeric).\cr
#'   \[ESP\] Tiempo transcurrido en segundos (numeric).
#' @export
#'
#' @examples
#' t0 <- gh_tic()
#' elapsed <- gh_toc(t0)
gh_toc <- function(t0) {
  as.numeric((proc.time() - t0)[["elapsed"]])
}

# Internal formatter: seconds -> human-readable string
.fmt_time <- function(seconds) {
  if (seconds < 60) {
    paste0(round(seconds, 1), "s")
  } else if (seconds < 3600) {
    paste0(floor(seconds / 60), "m ", round(seconds %% 60), "s")
  } else {
    h <- floor(seconds / 3600)
    m <- floor((seconds %% 3600) / 60)
    paste0(h, "h ", m, "m")
  }
}

# =============================================================================
# 5. FILESYSTEM
# =============================================================================

#' Create a directory if it does not exist
#'
#' \[EN\] Creates the full path (equivalent to `mkdir -p`) without error if it
#' already exists.
#'
#' \[ESP\] Crea la ruta completa (equivalente a `mkdir -p`) sin error si ya
#' existe.
#'
#' @param path \[EN\] Directory path to create.\cr
#'   \[ESP\] Ruta del directorio a crear.
#'
#' @return \[EN\] Invisible `path`.\cr
#'   \[ESP\] Invisible `path`.
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

#' Build an output path for a trait and layer
#'
#' \[EN\] Combines the config output base path with trait and layer
#' placeholders, and creates the directory if it does not exist.
#'
#' \[ESP\] Combina la ruta base de outputs del config con los placeholders de
#' trait y capa, y crea el directorio si no existe.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param type \[EN\] Path key in `rutas.outputs`, e.g. `"visualizacion"` or
#'   `"traits"`.\cr
#'   \[ESP\] Clave de ruta en `rutas.outputs`, p. ej. `"visualizacion"` o
#'   `"traits"`.
#' @param trait_id \[EN\] EFO/MONDO ID of the trait.\cr
#'   \[ESP\] ID EFO/MONDO del trait.
#' @param trait_name \[EN\] Human-readable trait name.\cr
#'   \[ESP\] Nombre legible del trait.
#' @param layer \[EN\] Layer name. If `NULL` the `{capa}` placeholder is not
#'   resolved.\cr
#'   \[ESP\] Nombre de la capa. Si es `NULL` no se resuelve `{capa}`.
#' @param create \[EN\] Logical. If `TRUE` (default) creates the directory.\cr
#'   \[ESP\] Logico. Si `TRUE` (por defecto) crea el directorio.
#'
#' @return \[EN\] Resolved path as a character string.\cr
#'   \[ESP\] Ruta resuelta como cadena de texto.
#' @export
#'
#' @examples
#' config <- load_config()
#' \dontrun{
#' build_output_path(config, "traits", "EFO_0001645", "alzheimer")
#' build_output_path(config, "visualizacion", "EFO_0001645", "alzheimer", "gwas_comun")
#' }
build_output_path <- function(config, type, trait_id, trait_name,
                               layer = NULL, create = TRUE) {
  base <- get_config(config, paste0("rutas.outputs.", type))
  if (is.null(base)) stop("Path not found in config: rutas.outputs.", type)
  path <- resolve_trait_path(base, trait_id, trait_name, layer)
  if (create) ensure_dir(path)
  path
}

# =============================================================================
# 6. ERROR HANDLING
# =============================================================================

#' Execute a pipeline step with error policy
#'
#' \[EN\] Wraps the execution of a function applying the policy defined in
#' `config$errores$politica`:
#' - `"parar"`: propagates the error (R default behaviour).
#' - `"continuar_trait"`: captures the error, logs it and returns `NULL`.
#' - `"continuar_run"`: same as `continuar_trait`.
#'
#' \[ESP\] Envuelve la ejecucion de una funcion aplicando la politica definida
#' en `config$errores$politica`:
#' - `"parar"`: propaga el error (comportamiento por defecto de R).
#' - `"continuar_trait"`: captura el error, lo registra y devuelve `NULL`.
#' - `"continuar_run"`: igual que `continuar_trait`.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param step \[EN\] Descriptive name of the step (used in the error
#'   message).\cr
#'   \[ESP\] Nombre descriptivo del paso (para el mensaje de error).
#' @param expr \[EN\] Expression to evaluate (passed unquoted, as in
#'   `tryCatch`).\cr
#'   \[ESP\] Expresion a evaluar (se pasa sin comillas, como en `tryCatch`).
#'
#' @return \[EN\] The result of `expr`, or `NULL` if an error was captured and
#'   the policy is not `"parar"`.\cr
#'   \[ESP\] El resultado de `expr`, o `NULL` si se capturo un error y la
#'   politica no es `"parar"`.
#' @export
#'
#' @examples
#' config <- load_config()
#' result <- try_step(config, "gene loading", {
#'   1 + 1
#' })
try_step <- function(config, step, expr) {
  politica <- get_config(config, "errores.politica", default = "parar")
  if (politica == "parar") {
    return(expr)
  }
  tryCatch(
    expr,
    error = function(e) {
      gh_log(config, "ERROR", "Failed in '", step, "': ", conditionMessage(e))
      NULL
    }
  )
}

# =============================================================================
# 7. OPTIONAL DEPENDENCIES (Suggests)
# =============================================================================

#' Check if a Suggests package is available
#'
#' \[EN\] Emits a clear message if the package is not installed, instead of a
#' cryptic error. Useful for modules that depend on Bioconductor or optional
#' packages.
#'
#' \[ESP\] Emite un mensaje claro si el paquete no esta instalado, en lugar de
#' un error criptico. Util para modulos que dependen de Bioconductor o paquetes
#' opcionales.
#'
#' @param pkg \[EN\] Package name.\cr
#'   \[ESP\] Nombre del paquete.
#' @param module \[EN\] Name of the module that requires it (for the
#'   message).\cr
#'   \[ESP\] Nombre del modulo que lo necesita (para el mensaje).
#'
#' @return \[EN\] `TRUE` if the package is available, `FALSE` otherwise (with
#'   an informative `message()`).\cr
#'   \[ESP\] `TRUE` si el paquete esta disponible, `FALSE` en caso contrario
#'   (con un `message()` informativo).
#' @export
#'
#' @examples
#' check_suggests("cli", "core_utils")
#' check_suggests("paqueteInexistente", "modulo_ejemplo")
check_suggests <- function(pkg, module) {
  if (requireNamespace(pkg, quietly = TRUE)) return(TRUE)
  message(
    "Package '", pkg, "' is required for '", module, "' but is not ",
    "installed.\n  Install it with: install.packages('", pkg, "')"
  )
  FALSE
}

# =============================================================================
# 8. SEED AND RESOURCES
# =============================================================================

#' Set the random seed from configuration
#'
#' \[EN\] Applies `set.seed()` with the value from `config$ejecucion$semilla`
#' to guarantee reproducibility in permutations, UMAP, clustering, etc.
#'
#' \[ESP\] Aplica `set.seed()` con el valor de `config$ejecucion$semilla` para
#' garantizar reproducibilidad en permutaciones, UMAP, clustering, etc.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @export
#'
#' @examples
#' config <- load_config()
#' gh_set_seed(config)
gh_set_seed <- function(config) {
  set.seed(config$ejecucion$semilla)
  invisible(NULL)
}

#' Run garbage collection between steps if configured
#'
#' \[EN\] Calls `gc()` only if `config$rendimiento$gc_entre_pasos` is `TRUE`.
#' Reduces RAM usage between heavy pipeline steps.
#'
#' \[ESP\] Llama a `gc()` solo si `config$rendimiento$gc_entre_pasos` es
#' `TRUE`. Reduce el uso de RAM entre pasos pesados del pipeline.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
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

#' Record an entry in the execution manifest
#'
#' \[EN\] Appends a line to the YAML manifest of the current execution with
#' the step name, status, time and optional message. The manifest is saved at
#' `outputs/{run_id}/global/parametros/manifest.yaml`.
#'
#' \[ESP\] Añade una linea al manifest YAML de la ejecucion actual con el
#' nombre del paso, estado, tiempo y mensaje opcional. El manifest se guarda en
#' `outputs/{run_id}/global/parametros/manifest.yaml`.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param step \[EN\] Name of the recorded step.\cr
#'   \[ESP\] Nombre del paso registrado.
#' @param status \[EN\] `"ok"`, `"error"` or `"omitido"`.\cr
#'   \[ESP\] `"ok"`, `"error"` o `"omitido"`.
#' @param time \[EN\] Duration in seconds (numeric). Can be `NULL`.\cr
#'   \[ESP\] Duracion en segundos (numeric). Puede ser `NULL`.
#' @param msg \[EN\] Optional additional message.\cr
#'   \[ESP\] Mensaje adicional opcional.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom yaml write_yaml
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' gh_manifest(config, "network_genes", "ok", time = 1.2)
#' gh_manifest(config, "analysis_latent", "omitido")
#' }
gh_manifest <- function(config, step, status, time = NULL, msg = NULL) {
  global_dir  <- get_config(config, "rutas.outputs.global")
  if (is.null(global_dir)) return(invisible(NULL))

  manifest_dir  <- file.path(global_dir, "parametros")
  manifest_path <- file.path(manifest_dir, "manifest.yaml")
  ensure_dir(manifest_dir)

  entry <- list(
    paso      = step,
    estado    = status,
    timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"),
    tiempo_s  = time,
    mensaje   = msg
  )

  # Read existing manifest and append the new entry
  current <- if (file.exists(manifest_path)) {
    yaml::read_yaml(manifest_path)
  } else {
    list()
  }
  current[[length(current) + 1L]] <- entry
  yaml::write_yaml(current, manifest_path)

  invisible(NULL)
}

# =============================================================================
# 10. HELPERS AND VALIDATIONS
# =============================================================================

# Format a number with thousands separator — e.g. 19247 -> "19,247"
#' @noRd
.fmt_num <- function(n) {
  formatC(as.integer(round(n)), format = "d", big.mark = ",")
}

#' Print trait processing start
#'
#' \[EN\] Prints a console line indicating that the pipeline has started
#' processing a specific trait.
#'
#' \[ESP\] Imprime en consola que el pipeline empieza a procesar un trait
#' concreto.
#'
#' @param trait_id \[EN\] EFO/MONDO identifier of the trait.\cr
#'   \[ESP\] Identificador EFO/MONDO del trait.
#' @param trait_name \[EN\] Human-readable trait name.\cr
#'   \[ESP\] Nombre legible del trait.
#' @param i \[EN\] Current trait index.\cr
#'   \[ESP\] Indice del trait actual.
#' @param n_total \[EN\] Total number of traits to process.\cr
#'   \[ESP\] Total de traits a procesar.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_text col_blue style_bold
#' @export
#'
#' @examples
#' gh_trait_start("EFO_0001645", "alzheimer", 12L, 844L)
gh_trait_start <- function(trait_id, trait_name, i, n_total) {
  cli::cli_text(
    "  {cli::col_blue('\u25B6')} Trait {i}/{n_total} \u00B7 ",
    "{cli::style_bold(trait_name)} ({trait_id})"
  )
  invisible(NULL)
}

#' Print trait processing result
#'
#' \[EN\] Prints the outcome of a processed trait together with its elapsed
#' time.
#'
#' \[ESP\] Imprime en consola el resultado de un trait con el tiempo
#' transcurrido.
#'
#' @param trait_name \[EN\] Human-readable trait name.\cr
#'   \[ESP\] Nombre legible del trait.
#' @param time \[EN\] Elapsed time in seconds.\cr
#'   \[ESP\] Tiempo transcurrido en segundos.
#' @param ok \[EN\] `TRUE` (default) shows a success symbol; `FALSE` shows an
#'   error symbol.\cr
#'   \[ESP\] `TRUE` (por defecto) muestra simbolo de exito; `FALSE` de error.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_text col_green col_red symbol
#' @export
#'
#' @examples
#' gh_trait_done("alzheimer", time = 12.4)
#' gh_trait_done("alzheimer", time = 3.1, ok = FALSE)
gh_trait_done <- function(trait_name, time, ok = TRUE) {
  sym   <- if (ok) cli::col_green(cli::symbol$tick) else cli::col_red(cli::symbol$cross)
  t_str <- .fmt_time(time)
  pad   <- strrep(" ", max(1L, 30L - nchar(trait_name)))
  cli::cli_text("  {sym} {trait_name}{pad}done in {t_str}")
  invisible(NULL)
}

#' Print that a pipeline step is skipped
#'
#' \[EN\] Prints a console line indicating that a step is disabled in the
#' current configuration.
#'
#' \[ESP\] Imprime en consola que un paso esta desactivado en la configuracion
#' actual.
#'
#' @param step \[EN\] Name of the skipped step.\cr
#'   \[ESP\] Nombre del paso omitido.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom cli cli_text col_grey
#' @export
#'
#' @examples
#' gh_skip("analysis_latent")
#' gh_skip("results_annotation")
gh_skip <- function(step) {
  pad <- strrep(" ", max(1L, 30L - nchar(step)))
  cli::cli_text(
    "  {cli::col_grey('\u2192')} {step}{pad}",
    "{cli::col_grey('[skipped]  \u25CB')}"
  )
  invisible(NULL)
}

#' Assert that a data.table is non-empty
#'
#' \[EN\] Throws an informative `stop()` if `dt` is `NULL` or has zero rows.
#'
#' \[ESP\] Lanza un `stop()` informativo si `dt` es `NULL` o tiene cero filas.
#'
#' @param dt \[EN\] Object to check (expected to be a `data.table`).\cr
#'   \[ESP\] Objeto a comprobar (se espera un `data.table`).
#' @param name \[EN\] Dataset name used in the error message.\cr
#'   \[ESP\] Nombre del dataset usado en el mensaje de error.
#'
#' @return \[EN\] Invisible `TRUE` if the check passes.\cr
#'   \[ESP\] Invisible `TRUE` si la comprobacion pasa.
#' @export
#'
#' @examples
#' assert_non_empty(mtcars, "mtcars")
assert_non_empty <- function(dt, name) {
  if (is.null(dt) || nrow(dt) == 0L) {
    stop("Dataset '", name, "' is NULL or has 0 rows.")
  }
  invisible(TRUE)
}

#' Assert that a file exists on disk
#'
#' \[EN\] Throws an informative `stop()` if the file at `path` does not exist.
#'
#' \[ESP\] Lanza un `stop()` informativo si el archivo en `path` no existe.
#'
#' @param path \[EN\] File path to check.\cr
#'   \[ESP\] Ruta del archivo a comprobar.
#'
#' @return \[EN\] Invisible `TRUE` if the check passes.\cr
#'   \[ESP\] Invisible `TRUE` si la comprobacion pasa.
#' @export
#'
#' @examples
#' \dontrun{
#' assert_file_exists("inputs/genes/part-0.parquet")
#' }
assert_file_exists <- function(path) {
  if (!file.exists(path)) {
    stop("File not found: ", path)
  }
  invisible(TRUE)
}

# =============================================================================
# 11. SESSION SNAPSHOT
# =============================================================================

#' Save session information to disk
#'
#' \[EN\] Captures `sessionInfo()` and writes it to
#' `outputs/{run_id}/global/parametros/session_info.txt` for reproducibility
#' auditing.
#'
#' \[ESP\] Captura `sessionInfo()` y lo escribe en
#' `outputs/{run_id}/global/parametros/session_info.txt` para auditoria de
#' reproducibilidad.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#'
#' @return \[EN\] Invisible path to the written file, or `NULL` if the global
#'   output directory is not configured.\cr
#'   \[ESP\] Invisible ruta del archivo escrito, o `NULL` si el directorio de
#'   output global no esta configurado.
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' gh_session_snapshot(config)
#' }
gh_session_snapshot <- function(config) {
  global_dir <- get_config(config, "rutas.outputs.global")
  if (is.null(global_dir)) return(invisible(NULL))

  snap_dir  <- file.path(global_dir, "parametros")
  snap_path <- file.path(snap_dir, "session_info.txt")
  ensure_dir(snap_dir)

  info <- utils::capture.output(utils::sessionInfo())
  writeLines(info, snap_path)

  invisible(snap_path)
}

# =============================================================================
# 12. MOVING ETA
# =============================================================================

# Internal environment that stores the rolling window of per-trait durations
.gh_eta_env <- new.env(parent = emptyenv())
.gh_eta_env$times  <- numeric(0L)
.gh_eta_env$window <- 10L

#' Initialise the moving ETA tracker
#'
#' \[EN\] Resets the rolling window used to estimate time remaining. Call once
#' before the trait loop, before the first [gh_progress_init()] call.
#'
#' \[ESP\] Reinicia la ventana deslizante usada para estimar el tiempo
#' restante. Llamar una vez antes del bucle de traits, antes de
#' [gh_progress_init()].
#'
#' @param window \[EN\] Number of most-recent traits used to compute the
#'   rolling mean (default `10`).\cr
#'   \[ESP\] Numero de traits recientes usados para calcular la media movil
#'   (por defecto `10`).
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @export
#'
#' @examples
#' gh_eta_init()
#' gh_eta_init(window = 5L)
gh_eta_init <- function(window = 10L) {
  .gh_eta_env$times  <- numeric(0L)
  .gh_eta_env$window <- as.integer(window)
  invisible(NULL)
}

#' Record the duration of a completed trait
#'
#' \[EN\] Appends `seconds` to the rolling window. Older values beyond
#' `window` are dropped automatically. Call after each trait completes,
#' before [gh_progress_tick()].
#'
#' \[ESP\] Añade `seconds` a la ventana deslizante. Los valores mas antiguos
#' que superen `window` se descartan automaticamente. Llamar tras completar
#' cada trait, antes de [gh_progress_tick()].
#'
#' @param seconds \[EN\] Elapsed time for the completed trait in seconds.\cr
#'   \[ESP\] Tiempo transcurrido del trait completado en segundos.
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom utils tail
#' @export
#'
#' @examples
#' gh_eta_init()
#' gh_eta_record(4.2)
#' gh_eta_record(3.8)
gh_eta_record <- function(seconds) {
  times <- c(.gh_eta_env$times, seconds)
  w     <- .gh_eta_env$window
  .gh_eta_env$times <- if (length(times) > w) tail(times, w) else times
  invisible(NULL)
}

#' Get the current ETA estimate
#'
#' \[EN\] Returns the estimated remaining time in seconds, computed as the
#' rolling mean of recorded durations multiplied by the number of remaining
#' traits. Returns `NA_real_` if no durations have been recorded yet or
#' `n_remaining` is zero.
#'
#' \[ESP\] Devuelve el tiempo restante estimado en segundos, calculado como
#' la media movil de las duraciones registradas multiplicada por el numero de
#' traits restantes. Devuelve `NA_real_` si aun no se han registrado
#' duraciones o `n_remaining` es cero.
#'
#' @param n_remaining \[EN\] Number of traits still to process.\cr
#'   \[ESP\] Numero de traits pendientes de procesar.
#'
#' @return \[EN\] Estimated seconds remaining (numeric), or `NA_real_`.\cr
#'   \[ESP\] Segundos restantes estimados (numeric), o `NA_real_`.
#' @export
#'
#' @examples
#' gh_eta_init()
#' gh_eta_record(4.0)
#' gh_eta_record(5.0)
#' gh_eta_get(100L)
gh_eta_get <- function(n_remaining) {
  times <- .gh_eta_env$times
  if (length(times) == 0L || n_remaining <= 0L) return(NA_real_)
  mean(times) * n_remaining
}

# =============================================================================
# 13. CHECKPOINTS
# =============================================================================

#' Save a trait result to the run checkpoint
#'
#' \[EN\] Appends an entry for `trait_id` to
#' `outputs/{run_id}/global/checkpoint.yaml`. If the file does not exist it
#' is created. Enables resuming interrupted runs by skipping already-completed
#' traits.
#'
#' \[ESP\] Añade una entrada para `trait_id` en
#' `outputs/{run_id}/global/checkpoint.yaml`. Si el archivo no existe lo crea.
#' Permite reanudar ejecuciones interrumpidas saltando los traits ya
#' completados.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param trait_id \[EN\] EFO/MONDO identifier of the trait.\cr
#'   \[ESP\] Identificador EFO/MONDO del trait.
#' @param status \[EN\] `"ok"` or `"error"`.\cr
#'   \[ESP\] `"ok"` o `"error"`.
#' @param msg \[EN\] Optional message (e.g. error text).\cr
#'   \[ESP\] Mensaje opcional (p.ej. texto del error).
#'
#' @return \[EN\] Invisible `NULL`.\cr
#'   \[ESP\] Invisible `NULL`.
#' @importFrom yaml read_yaml write_yaml
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' gh_checkpoint_save(config, "EFO_0001645", "ok")
#' gh_checkpoint_save(config, "EFO_0000182", "error", msg = "empty dataset")
#' }
gh_checkpoint_save <- function(config, trait_id, status, msg = NULL) {
  global_dir <- get_config(config, "rutas.outputs.global")
  if (is.null(global_dir)) return(invisible(NULL))

  cp_path <- file.path(global_dir, "checkpoint.yaml")
  ensure_dir(global_dir)

  entry <- list(
    trait_id  = trait_id,
    status    = status,
    timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"),
    msg       = msg
  )

  current <- if (file.exists(cp_path)) yaml::read_yaml(cp_path) else list()
  current[[length(current) + 1L]] <- entry
  yaml::write_yaml(current, cp_path)

  invisible(NULL)
}

#' Load completed trait IDs from the run checkpoint
#'
#' \[EN\] Reads the checkpoint file of the current run and returns a character
#' vector of trait IDs that completed with `status == "ok"`.
#'
#' \[ESP\] Lee el archivo de checkpoint de la ejecucion actual y devuelve un
#' vector de character con los trait_id que completaron con `status == "ok"`.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#'
#' @return \[EN\] Character vector of completed trait IDs (empty if no
#'   checkpoint exists).\cr
#'   \[ESP\] Vector de character con los trait_id completados (vacio si no
#'   existe checkpoint).
#' @importFrom yaml read_yaml
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' done   <- gh_checkpoint_load(config)
#' }
gh_checkpoint_load <- function(config) {
  global_dir <- get_config(config, "rutas.outputs.global")
  if (is.null(global_dir)) return(character(0L))

  cp_path <- file.path(global_dir, "checkpoint.yaml")
  if (!file.exists(cp_path)) return(character(0L))

  entries <- yaml::read_yaml(cp_path)
  ok      <- vapply(entries, function(e) isTRUE(e$status == "ok"), logical(1L))
  vapply(entries[ok], function(e) e$trait_id, character(1L))
}

#' Check whether a trait is already done in the checkpoint
#'
#' \[EN\] Returns `TRUE` if `trait_id` is present in the checkpoint with
#' `status == "ok"`. Convenience wrapper around [gh_checkpoint_load()].
#'
#' \[ESP\] Devuelve `TRUE` si `trait_id` esta en el checkpoint con
#' `status == "ok"`. Wrapper conveniente sobre [gh_checkpoint_load()].
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param trait_id \[EN\] EFO/MONDO identifier of the trait.\cr
#'   \[ESP\] Identificador EFO/MONDO del trait.
#'
#' @return \[EN\] `TRUE` if the trait is already completed, `FALSE`
#'   otherwise.\cr
#'   \[ESP\] `TRUE` si el trait ya esta completado, `FALSE` en caso contrario.
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' if (gh_checkpoint_is_done(config, "EFO_0001645")) {
#'   message("Skipping already completed trait.")
#' }
#' }
gh_checkpoint_is_done <- function(config, trait_id) {
  trait_id %in% gh_checkpoint_load(config)
}
