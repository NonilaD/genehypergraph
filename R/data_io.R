# ============================================================
#  CAPA 2 — DATA  |  data_io.R
#  Lectura de Parquet via DuckDB con seleccion de columnas
# ============================================================

# ── helpers internos ──────────────────────────────────────────

#' @noRd
.duckdb_con <- function() {
  DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
}

#' @noRd
.duckdb_read <- function(con, path, cols) {
  glob     <- file.path(path, "*.parquet")
  cols_sql <- paste(cols, collapse = ", ")
  query    <- sprintf("SELECT %s FROM read_parquet('%s')", cols_sql, glob)
  data.table::as.data.table(DBI::dbGetQuery(con, query))
}

# ── funciones publicas ────────────────────────────────────────

#' Leer genes protein-coding desde Parquet
#'
#' Lee el dataset de genes de Open Targets Target y devuelve
#' un subconjunto de columnas esenciales como `data.table`.
#'
#' @param config Lista de configuracion creada por [load_config()].
#'
#' @return `data.table` con columnas:
#'   `id`, `approvedSymbol`, `biotype`, `approvedName`, `genomicLocation`.
#'
#' @export
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom data.table as.data.table
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' genes  <- read_genes(config)
#' }
read_genes <- function(config) {
  path <- get_config(config, "rutas.inputs.genes")
  cache_or_compute(config, cache_key("genes", path), {
    con <- .duckdb_con()
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    .duckdb_read(con, path, c(
      "id", "approvedSymbol", "biotype", "approvedName", "genomicLocation"
    ))
  })
}

#' Leer interactoma proteina-proteina desde Parquet
#'
#' Lee el dataset de interacciones PPI (IntAct, STRING, etc.)
#' y devuelve las columnas clave como `data.table`.
#'
#' @param config Lista de configuracion creada por [load_config()].
#'
#' @return `data.table` con columnas:
#'   `sourceDatabase`, `targetA`, `intA`, `targetB`, `intB`, `count`, `scoring`.
#'
#' @export
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom data.table as.data.table
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' ppi    <- read_interactoma(config)
#' }
read_interactoma <- function(config) {
  path <- get_config(config, "rutas.inputs.interactoma")
  cache_or_compute(config, cache_key("interactoma", path), {
    con <- .duckdb_con()
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    .duckdb_read(con, path, c(
      "sourceDatabase", "targetA", "intA",
      "targetB",        "intB",    "count", "scoring"
    ))
  })
}

#' Leer asociaciones GWAS de variantes comunes desde Parquet
#'
#' Lee los credible sets de GWAS (Open Targets) y devuelve
#' las columnas de asociacion gen-enfermedad como `data.table`.
#'
#' @param config Lista de configuracion creada por [load_config()].
#'
#' @return `data.table` con columnas de asociacion GWAS:
#'   `datasourceId`, `targetId`, `diseaseId`, `diseaseFromSource`,
#'   `score`, `pValueExponent`, `pValueMantissa`, `beta`, `oddsRatio`,
#'   `variantId`, `variantRsId`, `studyId`, `studyLocusId`,
#'   `datatypeId`, `resourceScore`, `directionOnTrait`.
#'
#' @export
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom data.table as.data.table
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' gwas   <- read_gwas_common(config)
#' }
read_gwas_common <- function(config) {
  path <- get_config(config, "rutas.inputs.comun")
  cache_or_compute(config, cache_key("gwas_comun", path), {
    con <- .duckdb_con()
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    .duckdb_read(con, path, c(
      "datasourceId",    "targetId",        "diseaseId",
      "diseaseFromSource",
      "score",           "pValueExponent",  "pValueMantissa",
      "beta",            "oddsRatio",
      "variantId",       "variantRsId",
      "studyId",         "studyLocusId",
      "datatypeId",      "resourceScore",   "directionOnTrait"
    ))
  })
}

#' Leer asociaciones GWAS de variantes raras desde Parquet
#'
#' Lee los datos de variantes raras (EVA, ClinVar) de Open Targets
#' y devuelve las columnas de asociacion gen-enfermedad como `data.table`.
#'
#' @param config Lista de configuracion creada por [load_config()].
#'
#' @return `data.table` con columnas:
#'   `datasourceId`, `targetId`, `diseaseId`, `diseaseFromSource`,
#'   `score`, `clinicalSignificances`, `variantId`, `variantRsId`,
#'   `variantFromSourceId`, `variantFunctionalConsequenceId`,
#'   `alleleOrigins`, `datatypeId`, `confidence`, `directionOnTrait`.
#'
#' @export
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom data.table as.data.table
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' rara   <- read_gwas_rare(config)
#' }
read_gwas_rare <- function(config) {
  path <- get_config(config, "rutas.inputs.rara")
  cache_or_compute(config, cache_key("gwas_rara", path), {
    con <- .duckdb_con()
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    .duckdb_read(con, path, c(
      "datasourceId",                  "targetId",
      "diseaseId",                     "diseaseFromSource",
      "score",                         "clinicalSignificances",
      "variantId",                     "variantRsId",
      "variantFromSourceId",           "variantFunctionalConsequenceId",
      "alleleOrigins",                 "datatypeId",
      "confidence",                    "directionOnTrait"
    ))
  })
}

#' Leer datos de expresion genica por tejido desde Parquet
#'
#' Lee el dataset de expresion (GTEx / HPA) de Open Targets
#' y devuelve todas las columnas como `data.table`.
#'
#' @param config Lista de configuracion creada por [load_config()].
#'
#' @return `data.table` con columnas:
#'   `id`, `efo_code`, `label`, `rna_value`, `rna_zscore`, `rna_level`,
#'   `rna_unit`, `organs`, `anatomical_systems`, `protein_level`,
#'   `protein_reliability`.
#'
#' @export
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom data.table as.data.table
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' expr   <- read_expression(config)
#' }
read_expression <- function(config) {
  path <- get_config(config, "rutas.inputs.expresion")
  cache_or_compute(config, cache_key("expresion", path), {
    con <- .duckdb_con()
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    .duckdb_read(con, path, c(
      "id",                  "efo_code",           "label",
      "rna_value",           "rna_zscore",         "rna_level",
      "rna_unit",            "organs",             "anatomical_systems",
      "protein_level",       "protein_reliability"
    ))
  })
}

#' Leer ontologia de enfermedades desde Parquet
#'
#' Lee el dataset de enfermedades de Open Targets (EFO/MONDO/DOID)
#' y devuelve las columnas de ontologia como `data.table`.
#'
#' @param config Lista de configuracion creada por [load_config()].
#'
#' @return `data.table` con columnas:
#'   `id`, `name`, `description`, `therapeuticAreas`,
#'   `parents`, `ancestors`, `children`, `synonyms`.
#'
#' @export
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom data.table as.data.table
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' dis    <- read_diseases(config)
#' }
read_diseases <- function(config) {
  path <- get_config(config, "rutas.inputs.enfermedades")
  cache_or_compute(config, cache_key("enfermedades", path), {
    con <- .duckdb_con()
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    .duckdb_read(con, path, c(
      "id",               "name",             "description",
      "therapeuticAreas", "parents",          "ancestors",
      "children",         "synonyms"
    ))
  })
}
