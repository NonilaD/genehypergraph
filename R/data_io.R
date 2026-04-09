# ============================================================
#  LAYER 2 — DATA  |  data_io.R
#  Parquet reading via DuckDB with column pushdown
# ============================================================

# ── internal helpers ──────────────────────────────────────────

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

# ── public functions ──────────────────────────────────────────

#' Read protein-coding genes from Parquet
#'
#' \[EN\] Reads the Open Targets Target gene dataset and returns a subset of
#' essential columns as a `data.table`.
#'
#' \[ESP\] Lee el dataset de genes de Open Targets Target y devuelve un
#' subconjunto de columnas esenciales como `data.table`.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#'
#' @return \[EN\] `data.table` with columns:
#'   `id`, `approvedSymbol`, `biotype`, `approvedName`, `genomicLocation`.\cr
#'   \[ESP\] `data.table` con columnas:
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

#' Read protein-protein interactome from Parquet
#'
#' \[EN\] Reads the PPI interaction dataset (IntAct, STRING, etc.) and returns
#' the key columns as a `data.table`.
#'
#' \[ESP\] Lee el dataset de interacciones PPI (IntAct, STRING, etc.) y
#' devuelve las columnas clave como `data.table`.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#'
#' @return \[EN\] `data.table` with columns:
#'   `sourceDatabase`, `targetA`, `intA`, `targetB`, `intB`, `count`,
#'   `scoring`.\cr
#'   \[ESP\] `data.table` con columnas:
#'   `sourceDatabase`, `targetA`, `intA`, `targetB`, `intB`, `count`,
#'   `scoring`.
#'
#' @export
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom data.table as.data.table
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' ppi    <- read_interactome(config)
#' }
read_interactome <- function(config) {
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

#' Read common variant GWAS associations from Parquet
#'
#' \[EN\] Reads the GWAS credible sets (Open Targets) and returns the
#' gene-disease association columns as a `data.table`.
#'
#' \[ESP\] Lee los credible sets de GWAS (Open Targets) y devuelve las columnas
#' de asociacion gen-enfermedad como `data.table`.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#'
#' @return \[EN\] `data.table` with GWAS association columns:
#'   `datasourceId`, `targetId`, `diseaseId`, `diseaseFromSource`,
#'   `score`, `pValueExponent`, `pValueMantissa`, `beta`, `oddsRatio`,
#'   `variantId`, `variantRsId`, `studyId`, `studyLocusId`,
#'   `datatypeId`, `resourceScore`, `directionOnTrait`.\cr
#'   \[ESP\] `data.table` con columnas de asociacion GWAS:
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

#' Read rare variant GWAS associations from Parquet
#'
#' \[EN\] Reads the rare variant data (EVA, ClinVar) from Open Targets and
#' returns the gene-disease association columns as a `data.table`.
#'
#' \[ESP\] Lee los datos de variantes raras (EVA, ClinVar) de Open Targets y
#' devuelve las columnas de asociacion gen-enfermedad como `data.table`.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#'
#' @return \[EN\] `data.table` with columns:
#'   `datasourceId`, `targetId`, `diseaseId`, `diseaseFromSource`,
#'   `score`, `clinicalSignificances`, `variantId`, `variantRsId`,
#'   `variantFromSourceId`, `variantFunctionalConsequenceId`,
#'   `alleleOrigins`, `datatypeId`, `confidence`, `directionOnTrait`.\cr
#'   \[ESP\] `data.table` con columnas:
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

#' Read tissue gene expression data from Parquet
#'
#' \[EN\] Reads the expression dataset (GTEx / HPA) from Open Targets and
#' returns all columns as a `data.table`.
#'
#' \[ESP\] Lee el dataset de expresion (GTEx / HPA) de Open Targets y devuelve
#' todas las columnas como `data.table`.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#'
#' @return \[EN\] `data.table` with columns:
#'   `id`, `efo_code`, `label`, `rna_value`, `rna_zscore`, `rna_level`,
#'   `rna_unit`, `organs`, `anatomical_systems`, `protein_level`,
#'   `protein_reliability`.\cr
#'   \[ESP\] `data.table` con columnas:
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

#' Read disease ontology from Parquet
#'
#' \[EN\] Reads the Open Targets disease dataset (EFO/MONDO/DOID) and returns
#' the ontology columns as a `data.table`.
#'
#' \[ESP\] Lee el dataset de enfermedades de Open Targets (EFO/MONDO/DOID) y
#' devuelve las columnas de ontologia como `data.table`.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#'
#' @return \[EN\] `data.table` with columns:
#'   `id`, `name`, `description`, `therapeuticAreas`,
#'   `parents`, `ancestors`, `children`, `synonyms`.\cr
#'   \[ESP\] `data.table` con columnas:
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
