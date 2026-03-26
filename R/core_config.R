# =============================================================================
# core_config.R — Carga, validación y acceso a la configuración del pipeline
# =============================================================================

# -----------------------------------------------------------------------------
# Claves obligatorias de primer nivel
# -----------------------------------------------------------------------------
.REQUIRED_KEYS <- c(
  "schema", "ejecucion", "rutas", "pipeline", "capas", "filtros", "traits",
  "propagacion", "comunidades", "metricas_red", "integracion", "ranking",
  "significancia", "latent", "hypergraph", "anotacion",
  "visualizacion", "export", "cache", "rendimiento", "errores", "logging"
)

# -----------------------------------------------------------------------------
# Merge recursivo de listas
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
# Validación básica de la config cargada
# -----------------------------------------------------------------------------
.validate_config <- function(config) {

  claves_faltantes <- setdiff(.REQUIRED_KEYS, names(config))
  if (length(claves_faltantes) > 0) {
    stop(
      "config_default.yaml incompleto. Faltan secciones: ",
      paste(claves_faltantes, collapse = ", ")
    )
  }

  if (!is.numeric(config$schema$version)) {
    stop("schema.version debe ser un numero entero.")
  }

  if (!is.numeric(config$ejecucion$semilla)) {
    stop("ejecucion.semilla debe ser un numero entero.")
  }

  alpha <- config$propagacion$alpha
  if (!is.numeric(alpha) || alpha <= 0 || alpha >= 1) {
    stop("propagacion.alpha debe estar en (0, 1).")
  }

  for (capa in names(config$capas)) {
    peso <- config$capas[[capa]]$peso
    if (!is.numeric(peso) || peso < 0) {
      stop("capas.", capa, ".peso debe ser un numero >= 0.")
    }
  }

  politicas_validas <- c("parar", "continuar_trait", "continuar_run")
  if (!config$errores$politica %in% politicas_validas) {
    stop(
      "errores.politica debe ser una de: ",
      paste(politicas_validas, collapse = ", ")
    )
  }

  invisible(TRUE)
}

# -----------------------------------------------------------------------------
# Resolución de run_id en rutas de output
#
# Solo resuelve {run_id}. Los placeholders {trait_id}, {trait_name} y {capa}
# se resuelven en tiempo de ejecución por trait mediante resolve_trait_path().
# -----------------------------------------------------------------------------
.resolve_run_paths <- function(config, run_id) {
  config$rutas$outputs <- lapply(config$rutas$outputs, function(p) {
    gsub("{run_id}", run_id, p, fixed = TRUE)
  })
  config
}

# =============================================================================
# API pública
# =============================================================================

#' Cargar la configuración del pipeline
#'
#' Carga el YAML por defecto del paquete y, opcionalmente, lo fusiona con un
#' YAML de usuario mediante merge recursivo: las claves del usuario prevalecen,
#' pero las no especificadas conservan el valor por defecto.
#'
#' Las rutas de output que contienen `{run_id}` se resuelven en este paso.
#' Los placeholders `{trait_id}`, `{trait_name}` y `{capa}` se resuelven
#' después, por trait, mediante [resolve_trait_path()].
#'
#' @param user_config Ruta al YAML del usuario. Si es `NULL` se usan solo los
#'   valores por defecto.
#' @param run_id Identificador de la ejecución (cadena de texto). Si es `NULL`
#'   se genera automáticamente con la fecha y hora actuales
#'   (`"YYYYMMDD_HHMMSS"`).
#' @param validate Lógico. Si `TRUE` (por defecto) valida las claves
#'   obligatorias y los rangos de los parámetros críticos.
#'
#' @return Lista nombrada con la configuración resultante. Incluye el campo
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

  # 1. Config por defecto del paquete
  default_path <- system.file("config_default.yaml", package = "genehypergraph")
  if (!nzchar(default_path)) {
    stop("No se encontro config_default.yaml en el paquete instalado.")
  }
  config <- yaml::read_yaml(default_path)

  # 2. Merge con config del usuario (recursivo)
  if (!is.null(user_config)) {
    if (!file.exists(user_config)) {
      stop("Archivo de configuracion no encontrado: ", user_config)
    }
    config <- .merge_config(config, yaml::read_yaml(user_config))
  }

  # 3. Validación
  if (validate) {
    .validate_config(config)
  }

  # 4. Generar run_id y resolver {run_id} en rutas
  if (is.null(run_id)) {
    run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  }
  config$run_id <- run_id
  config <- .resolve_run_paths(config, run_id)

  config
}

#' Acceder a un valor de la configuración por ruta de claves
#'
#' Permite acceder a valores anidados de la configuración usando una cadena de
#' claves separadas por `"."` sin necesidad de encadenar `$`.
#'
#' @param config Lista de configuración devuelta por [load_config()].
#' @param path Cadena de claves separadas por `"."`,
#'   p. ej. `"comunidades.recluster.activar"`.
#' @param default Valor devuelto si la clave no existe. Por defecto `NULL`.
#'
#' @return El valor almacenado en esa ruta, o `default` si no existe.
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

#' Comprobar si un paso del pipeline está activo
#'
#' Wrapper sobre [get_config()] para consultar `pipeline.<paso>`. Funciona
#' tanto si el valor es un escalar lógico (`true`) como si es una lista con
#' el campo `activar` (p. ej. `analysis_integration`).
#'
#' @param config Lista de configuración devuelta por [load_config()].
#' @param paso Nombre del paso tal como aparece en la sección `pipeline` del
#'   YAML, p. ej. `"analysis_latent"` o `"results_visualization"`.
#'
#' @return `TRUE` si el paso está activo, `FALSE` en caso contrario.
#' @export
#'
#' @examples
#' config <- load_config()
#' pipeline_activo(config, "analysis_communities")
#' pipeline_activo(config, "analysis_integration")
#' pipeline_activo(config, "analysis_latent")
pipeline_activo <- function(config, paso) {
  val <- get_config(config, paste0("pipeline.", paso), default = FALSE)
  if (is.list(val)) isTRUE(val$activar) else isTRUE(val)
}

#' Resolver placeholders de trait y capa en una ruta de output
#'
#' Sustituye `{trait_id}`, `{trait_name}` y `{capa}` en las rutas de output
#' que los contengan. Se llama en tiempo de ejecución para cada trait,
#' después de [load_config()].
#'
#' @param path Cadena con la ruta a resolver (normalmente obtenida con
#'   [get_config()]).
#' @param trait_id ID EFO/MONDO del trait, p. ej. `"EFO_0001645"`.
#' @param trait_name Nombre legible del trait, p. ej. `"alzheimer"`.
#' @param capa Nombre de la capa, p. ej. `"gwas_comun"` o `"integracion"`.
#'   Si es `NULL` no se resuelve `{capa}`.
#'
#' @return La ruta con los placeholders sustituidos.
#' @export
#'
#' @examples
#' config <- load_config()
#' ruta <- get_config(config, "rutas.outputs.visualizacion")
#' resolve_trait_path(ruta, "EFO_0001645", "alzheimer", "gwas_comun")
resolve_trait_path <- function(path, trait_id, trait_name, capa = NULL) {
  path <- gsub("{trait_id}",   trait_id,   path, fixed = TRUE)
  path <- gsub("{trait_name}", trait_name, path, fixed = TRUE)
  if (!is.null(capa)) {
    path <- gsub("{capa}", capa, path, fixed = TRUE)
  }
  path
}
