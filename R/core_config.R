# =============================================================================
# core_config.R — Load, validation and access to pipeline configuration
# =============================================================================

# -----------------------------------------------------------------------------
# Required top-level keys
# -----------------------------------------------------------------------------
.REQUIRED_KEYS <- c(
  "schema", "ejecucion", "rutas", "pipeline", "capas", "filtros", "traits",
  "propagacion", "comunidades", "metricas_red", "integracion", "ranking",
  "significancia", "latent", "hypergraph", "anotacion",
  "visualizacion", "export", "cache", "rendimiento", "errores", "logging"
)

# -----------------------------------------------------------------------------
# Recursive list merge
# -----------------------------------------------------------------------------
.merge_config <- function(base, override) {
  for (key in names(override)) {
    if (is.list(override[[key]]) && is.list(base[[key]])) {
      base[[key]] <- .merge_config(base[[key]], override[[key]])
    } else {
      base[[key]] <- override[[key]]
    }
  }
  base
}

# -----------------------------------------------------------------------------
# Basic validation of the loaded config
# -----------------------------------------------------------------------------
.validate_config <- function(config) {

  missing_keys <- setdiff(.REQUIRED_KEYS, names(config))
  if (length(missing_keys) > 0) {
    stop(
      "config_default.yaml is incomplete. Missing sections: ",
      paste(missing_keys, collapse = ", ")
    )
  }

  if (!is.numeric(config$schema$version)) {
    stop("schema.version must be an integer number.")
  }

  if (!is.numeric(config$ejecucion$semilla)) {
    stop("ejecucion.semilla must be an integer number.")
  }

  alpha <- config$propagacion$alpha
  if (!is.numeric(alpha) || alpha <= 0 || alpha >= 1) {
    stop("propagacion.alpha must be in (0, 1).")
  }

  for (capa in names(config$capas)) {
    peso <- config$capas[[capa]]$peso
    if (!is.numeric(peso) || peso < 0) {
      stop("capas.", capa, ".peso must be a number >= 0.")
    }
  }

  valid_policies <- c("parar", "continuar_trait", "continuar_run")
  if (!config$errores$politica %in% valid_policies) {
    stop(
      "errores.politica must be one of: ",
      paste(valid_policies, collapse = ", ")
    )
  }

  invisible(TRUE)
}

# -----------------------------------------------------------------------------
# Resolve run_id in output paths
#
# Only resolves {run_id}. The placeholders {trait_id}, {trait_name} and {capa}
# are resolved at trait execution time via resolve_trait_path().
# -----------------------------------------------------------------------------
.resolve_run_paths <- function(config, run_id) {
  config$rutas$outputs <- lapply(config$rutas$outputs, function(p) {
    gsub("{run_id}", run_id, p, fixed = TRUE)
  })
  config
}

# =============================================================================
# Public API
# =============================================================================

#' Load pipeline configuration
#'
#' \[EN\] Loads the package default YAML and optionally merges it with a user
#' YAML via recursive merge: user keys take precedence, but unspecified keys
#' retain their default value. Output paths containing `{run_id}` are resolved
#' in this step. The placeholders `{trait_id}`, `{trait_name}` and `{capa}` are
#' resolved later, per trait, via [resolve_trait_path()].
#'
#' \[ESP\] Carga el YAML por defecto del paquete y, opcionalmente, lo fusiona
#' con un YAML de usuario mediante merge recursivo: las claves del usuario
#' prevalecen, pero las no especificadas conservan el valor por defecto. Las
#' rutas de output que contienen `{run_id}` se resuelven en este paso. Los
#' placeholders `{trait_id}`, `{trait_name}` y `{capa}` se resuelven despues,
#' por trait, mediante [resolve_trait_path()].
#'
#' @param user_config \[EN\] Path to the user YAML. If `NULL` only default
#'   values are used.\cr
#'   \[ESP\] Ruta al YAML del usuario. Si es `NULL` se usan solo los valores
#'   por defecto.
#' @param run_id \[EN\] Execution identifier (character string). If `NULL` it
#'   is generated automatically from the current date and time
#'   (`"YYYYMMDD_HHMMSS"`).\cr
#'   \[ESP\] Identificador de la ejecucion (cadena de texto). Si es `NULL` se
#'   genera automaticamente con la fecha y hora actuales (`"YYYYMMDD_HHMMSS"`).
#' @param validate \[EN\] Logical. If `TRUE` (default) validates required keys
#'   and ranges of critical parameters.\cr
#'   \[ESP\] Logico. Si `TRUE` (por defecto) valida las claves obligatorias y
#'   los rangos de los parametros criticos.
#'
#' @return \[EN\] Named list with the resulting configuration. Includes the
#'   `$run_id` field and output paths with `{run_id}` already resolved.\cr
#'   \[ESP\] Lista nombrada con la configuracion resultante. Incluye el campo
#'   `$run_id` y las rutas de output con `{run_id}` ya resuelto.
#'
#' @importFrom yaml read_yaml
#' @export
#'
#' @examples
#' config <- load_config()
#' config <- load_config(run_id = "prueba_01", validate = FALSE)
#' get_config(config, "propagacion.alpha")
#'
#' \dontrun{
#' # Solo si el archivo existe en disco
#' config <- load_config("mi_config.yaml")
#' }
load_config <- function(user_config = NULL, run_id = NULL, validate = TRUE) {

  # 1. Package default config
  default_path <- system.file("config_default.yaml", package = "genehypergraph")
  if (!nzchar(default_path)) {
    stop("config_default.yaml not found in the installed package.")
  }
  config <- yaml::read_yaml(default_path)

  # 2. Merge with user config (recursive)
  if (!is.null(user_config)) {
    if (!file.exists(user_config)) {
      stop("Configuration file not found: ", user_config)
    }
    config <- .merge_config(config, yaml::read_yaml(user_config))
  }

  # 3. Validation
  if (validate) {
    .validate_config(config)
  }

  # 4. Generate run_id and resolve {run_id} in paths
  if (is.null(run_id)) {
    run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  }
  config$run_id <- run_id
  config <- .resolve_run_paths(config, run_id)

  config
}

#' Access a configuration value by key path
#'
#' \[EN\] Allows access to nested configuration values using a dot-separated
#' key string without needing to chain `$`.
#'
#' \[ESP\] Permite acceder a valores anidados de la configuracion usando una
#' cadena de claves separadas por `"."` sin necesidad de encadenar `$`.
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param path \[EN\] Dot-separated key string, e.g.
#'   `"comunidades.recluster.activar"`.\cr
#'   \[ESP\] Cadena de claves separadas por `"."`, p. ej.
#'   `"comunidades.recluster.activar"`.
#' @param default \[EN\] Value returned if the key does not exist. Default
#'   `NULL`.\cr
#'   \[ESP\] Valor devuelto si la clave no existe. Por defecto `NULL`.
#'
#' @return \[EN\] The value stored at that path, or `default` if it does not
#'   exist.\cr
#'   \[ESP\] El valor almacenado en esa ruta, o `default` si no existe.
#' @export
#'
#' @examples
#' config <- load_config()
#' get_config(config, "propagacion.alpha")
#' get_config(config, "capas.gwas_rara.activar")
#' get_config(config, "clave.inexistente", default = FALSE)
get_config <- function(config, path, default = NULL) {
  keys <- strsplit(path, ".", fixed = TRUE)[[1]]
  val  <- config
  for (k in keys) {
    if (!is.list(val) || !k %in% names(val)) return(default)
    val <- val[[k]]
  }
  val
}

#' Check if a pipeline step is active
#'
#' \[EN\] Wrapper over [get_config()] to query `pipeline.<step>`. Works
#' whether the value is a scalar logical (`true`) or a list with an `activar`
#' field (e.g. `analysis_integration`).
#'
#' \[ESP\] Wrapper sobre [get_config()] para consultar `pipeline.<paso>`.
#' Funciona tanto si el valor es un escalar logico (`true`) como si es una
#' lista con el campo `activar` (p. ej. `analysis_integration`).
#'
#' @param config \[EN\] Configuration list returned by [load_config()].\cr
#'   \[ESP\] Lista de configuracion devuelta por [load_config()].
#' @param step \[EN\] Name of the step as it appears in the `pipeline` section
#'   of the YAML, e.g. `"analysis_latent"` or `"results_visualization"`.\cr
#'   \[ESP\] Nombre del paso tal como aparece en la seccion `pipeline` del
#'   YAML, p. ej. `"analysis_latent"` o `"results_visualization"`.
#'
#' @return \[EN\] `TRUE` if the step is active, `FALSE` otherwise.\cr
#'   \[ESP\] `TRUE` si el paso esta activo, `FALSE` en caso contrario.
#' @export
#'
#' @examples
#' config <- load_config()
#' pipeline_active(config, "analysis_communities")
#' pipeline_active(config, "analysis_integration")
#' pipeline_active(config, "analysis_latent")
pipeline_active <- function(config, step) {
  val <- get_config(config, paste0("pipeline.", step), default = FALSE)
  if (is.list(val)) isTRUE(val$activar) else isTRUE(val)
}

#' Resolve trait and layer placeholders in an output path
#'
#' \[EN\] Substitutes `{trait_id}`, `{trait_name}` and `{capa}` in output
#' paths that contain them. Called at execution time for each trait, after
#' [load_config()].
#'
#' \[ESP\] Sustituye `{trait_id}`, `{trait_name}` y `{capa}` en las rutas de
#' output que los contengan. Se llama en tiempo de ejecucion para cada trait,
#' despues de [load_config()].
#'
#' @param path \[EN\] String with the path to resolve (normally obtained with
#'   [get_config()]).\cr
#'   \[ESP\] Cadena con la ruta a resolver (normalmente obtenida con
#'   [get_config()]).
#' @param trait_id \[EN\] EFO/MONDO ID of the trait, e.g. `"EFO_0001645"`.\cr
#'   \[ESP\] ID EFO/MONDO del trait, p. ej. `"EFO_0001645"`.
#' @param trait_name \[EN\] Human-readable trait name, e.g. `"alzheimer"`.\cr
#'   \[ESP\] Nombre legible del trait, p. ej. `"alzheimer"`.
#' @param layer \[EN\] Layer name, e.g. `"gwas_comun"` or `"integracion"`. If
#'   `NULL` the `{capa}` placeholder is not resolved.\cr
#'   \[ESP\] Nombre de la capa, p. ej. `"gwas_comun"` o `"integracion"`. Si es
#'   `NULL` no se resuelve `{capa}`.
#'
#' @return \[EN\] The path with placeholders substituted.\cr
#'   \[ESP\] La ruta con los placeholders sustituidos.
#' @export
#'
#' @examples
#' config <- load_config()
#' ruta <- get_config(config, "rutas.outputs.viz_trait")
#' resolve_trait_path(ruta, "EFO_0001645", "alzheimer", "gwas_comun")
resolve_trait_path <- function(path, trait_id, trait_name, layer = NULL) {
  path <- gsub("{trait_id}",   trait_id,   path, fixed = TRUE)
  path <- gsub("{trait_name}", trait_name, path, fixed = TRUE)
  if (!is.null(layer)) {
    path <- gsub("{capa}", layer, path, fixed = TRUE)
  }
  path
}
