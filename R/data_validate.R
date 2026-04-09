# ============================================================
#  LAYER 2 — DATA  |  data_validate.R
#  Type, range, NA and cross-dataset integrity validation
# ============================================================

# ── internal helpers ──────────────────────────────────────────

#' @noRd
.vcols <- function(config, dt, required, dataset) {
  missing <- setdiff(required, names(dt))
  if (length(missing) > 0L) {
    stop(sprintf(
      "[%s] Required columns not found: %s",
      dataset, paste(missing, collapse = ", ")
    ))
  }
  invisible(TRUE)
}

#' @noRd
.vna <- function(dt, col, dataset) {
  n <- sum(is.na(dt[[col]]))
  if (n > 0L) {
    gh_warn(sprintf("[%s] %d NAs in key column '%s'", dataset, n, col))
  }
  invisible(n)
}

#' @noRd
.vpattern <- function(dt, col, pattern, dataset) {
  n_bad <- sum(!grepl(pattern, dt[[col]], perl = TRUE), na.rm = TRUE)
  if (n_bad > 0L) {
    gh_warn(sprintf(
      "[%s] %d values in '%s' do not match expected pattern",
      dataset, n_bad, col
    ))
  }
  invisible(n_bad)
}

#' @noRd
.vrange <- function(dt, col, min_val, max_val, dataset) {
  vals  <- dt[[col]]
  n_bad <- sum(vals < min_val | vals > max_val, na.rm = TRUE)
  if (n_bad > 0L) {
    gh_warn(sprintf(
      "[%s] %d values in '%s' outside range [%s, %s]",
      dataset, n_bad, col, min_val, max_val
    ))
  }
  invisible(n_bad)
}

# ── public functions ──────────────────────────────────────────

#' Validate gene dataset
#'
#' \[EN\] Checks columns, Ensembl pattern, biotypes and duplicates.
#'
#' \[ESP\] Comprueba columnas, patron Ensembl, biotipos y duplicados.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] `data.table` returned by [read_genes()].\cr
#'   \[ESP\] `data.table` devuelto por [read_genes()].
#'
#' @return \[EN\] `dt` unmodified (issues are logged via log).\cr
#'   \[ESP\] `dt` sin modificaciones (los problemas se registran via log).
#'
#' @export
#' @importFrom data.table uniqueN
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' genes  <- validate_genes(config, read_genes(config))
#' }
validate_genes <- function(config, dt) {
  required <- c("id", "approvedSymbol", "biotype", "approvedName", "genomicLocation")
  .vcols(config, dt, required, "genes")

  .vna(dt, "id",             "genes")
  .vna(dt, "approvedSymbol", "genes")
  .vpattern(dt, "id", "^ENSG\\d+", "genes")

  n_dup <- nrow(dt) - data.table::uniqueN(dt[["id"]])
  if (n_dup > 0L) {
    gh_warn(sprintf("[genes] %d duplicate IDs", n_dup))
  }

  n_pc <- sum(dt[["biotype"]] == "protein_coding", na.rm = TRUE)
  gh_log(config, "INFO", sprintf(
    "[genes] %d rows | %d protein_coding | %d unique IDs",
    nrow(dt), n_pc, data.table::uniqueN(dt[["id"]])
  ))
  dt
}

#' Validate interactome dataset
#'
#' \[EN\] Checks columns, Ensembl pattern in targetA/B and score range.
#'
#' \[ESP\] Comprueba columnas, patron Ensembl en targetA/B y rango del score.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] `data.table` returned by [read_interactome()].\cr
#'   \[ESP\] `data.table` devuelto por [read_interactome()].
#'
#' @return \[EN\] `dt` unmodified.\cr
#'   \[ESP\] `dt` sin modificaciones.
#'
#' @export
#' @importFrom data.table uniqueN
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' ppi    <- validate_interactome(config, read_interactome(config))
#' }
validate_interactome <- function(config, dt) {
  required <- c("sourceDatabase", "targetA", "intA", "targetB", "intB", "count", "scoring")
  .vcols(config, dt, required, "interactoma")

  .vna(dt, "targetA", "interactoma")
  .vna(dt, "targetB", "interactoma")
  .vpattern(dt, "targetA", "^ENSG\\d+", "interactoma")
  .vpattern(dt, "targetB", "^ENSG\\d+", "interactoma")
  .vrange(dt, "scoring", 0, 1, "interactoma")

  n_genes <- data.table::uniqueN(c(dt[["targetA"]], dt[["targetB"]]))
  gh_log(config, "INFO", sprintf(
    "[interactoma] %d interactions | %d unique genes | sources: %s",
    nrow(dt), n_genes,
    paste(unique(dt[["sourceDatabase"]]), collapse = ", ")
  ))
  dt
}

#' Validate common variant GWAS dataset
#'
#' \[EN\] Checks columns, Ensembl pattern, score range and disease coverage.
#'
#' \[ESP\] Comprueba columnas, patron Ensembl, rango de score y cobertura de
#' enfermedades.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] `data.table` returned by [read_gwas_common()].\cr
#'   \[ESP\] `data.table` devuelto por [read_gwas_common()].
#'
#' @return \[EN\] `dt` unmodified.\cr
#'   \[ESP\] `dt` sin modificaciones.
#'
#' @export
#' @importFrom data.table uniqueN
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' gwas   <- validate_gwas_common(config, read_gwas_common(config))
#' }
validate_gwas_common <- function(config, dt) {
  required <- c(
    "datasourceId", "targetId", "diseaseId", "score",
    "variantId", "studyId", "datatypeId"
  )
  .vcols(config, dt, required, "gwas_comun")

  .vna(dt, "targetId",  "gwas_comun")
  .vna(dt, "diseaseId", "gwas_comun")
  .vpattern(dt, "targetId", "^ENSG\\d+", "gwas_comun")
  .vrange(dt, "score", 0, 1, "gwas_comun")

  gh_log(config, "INFO", sprintf(
    "[gwas_comun] %d associations | %d genes | %d diseases | sources: %s",
    nrow(dt),
    data.table::uniqueN(dt[["targetId"]]),
    data.table::uniqueN(dt[["diseaseId"]]),
    paste(unique(dt[["datasourceId"]]), collapse = ", ")
  ))
  dt
}

#' Validate rare variant GWAS dataset
#'
#' \[EN\] Checks columns, Ensembl pattern, score range and clinical
#' significance.
#'
#' \[ESP\] Comprueba columnas, patron Ensembl, rango de score y significancia
#' clinica.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] `data.table` returned by [read_gwas_rare()].\cr
#'   \[ESP\] `data.table` devuelto por [read_gwas_rare()].
#'
#' @return \[EN\] `dt` unmodified.\cr
#'   \[ESP\] `dt` sin modificaciones.
#'
#' @export
#' @importFrom data.table uniqueN
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' rara   <- validate_gwas_rare(config, read_gwas_rare(config))
#' }
validate_gwas_rare <- function(config, dt) {
  required <- c(
    "datasourceId", "targetId", "diseaseId", "score",
    "clinicalSignificances", "variantId", "datatypeId"
  )
  .vcols(config, dt, required, "gwas_rara")

  .vna(dt, "targetId",  "gwas_rara")
  .vna(dt, "diseaseId", "gwas_rara")
  .vpattern(dt, "targetId", "^ENSG\\d+", "gwas_rara")
  .vrange(dt, "score", 0, 1, "gwas_rara")

  gh_log(config, "INFO", sprintf(
    "[gwas_rara] %d associations | %d genes | %d diseases | sources: %s",
    nrow(dt),
    data.table::uniqueN(dt[["targetId"]]),
    data.table::uniqueN(dt[["diseaseId"]]),
    paste(unique(dt[["datasourceId"]]), collapse = ", ")
  ))
  dt
}

#' Validate gene expression dataset
#'
#' \[EN\] Checks columns, Ensembl pattern, rna_value values and rna_level.
#'
#' \[ESP\] Comprueba columnas, patron Ensembl, valores de rna_value y
#' rna_level.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] `data.table` returned by [read_expression()].\cr
#'   \[ESP\] `data.table` devuelto por [read_expression()].
#'
#' @return \[EN\] `dt` unmodified.\cr
#'   \[ESP\] `dt` sin modificaciones.
#'
#' @export
#' @importFrom data.table uniqueN
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' expr   <- validate_expression(config, read_expression(config))
#' }
validate_expression <- function(config, dt) {
  required <- c("id", "efo_code", "label", "rna_value", "rna_level")
  .vcols(config, dt, required, "expresion")

  .vna(dt, "id",       "expresion")
  .vna(dt, "efo_code", "expresion")
  .vpattern(dt, "id", "^ENSG\\d+", "expresion")

  n_neg <- sum(dt[["rna_value"]] < 0, na.rm = TRUE)
  if (n_neg > 0L) {
    gh_warn(sprintf("[expresion] %d negative rna_value values", n_neg))
  }

  valid_levels <- c(-1L, 0L, 1L, 2L, 3L)
  n_bad_lvl <- sum(!dt[["rna_level"]] %in% valid_levels, na.rm = TRUE)
  if (n_bad_lvl > 0L) {
    gh_warn(sprintf(
      "[expresion] %d rna_level values outside expected range [-1, 3]", n_bad_lvl
    ))
  }

  gh_log(config, "INFO", sprintf(
    "[expresion] %d rows | %d genes | %d tissues",
    nrow(dt),
    data.table::uniqueN(dt[["id"]]),
    data.table::uniqueN(dt[["efo_code"]])
  ))
  dt
}

#' Validate disease ontology
#'
#' \[EN\] Checks columns and that id and name have no NAs.
#'
#' \[ESP\] Comprueba columnas y que id y name no tengan NAs.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] `data.table` returned by [read_diseases()].\cr
#'   \[ESP\] `data.table` devuelto por [read_diseases()].
#'
#' @return \[EN\] `dt` unmodified.\cr
#'   \[ESP\] `dt` sin modificaciones.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' dis    <- validate_diseases(config, read_diseases(config))
#' }
validate_diseases <- function(config, dt) {
  required <- c("id", "name", "therapeuticAreas", "parents", "ancestors")
  .vcols(config, dt, required, "enfermedades")

  .vna(dt, "id",   "enfermedades")
  .vna(dt, "name", "enfermedades")

  gh_log(config, "INFO", sprintf(
    "[enfermedades] %d ontology terms", nrow(dt)
  ))
  dt
}

#' Validate cross-dataset integrity
#'
#' \[EN\] Checks coverage between datasets: genes in interactome, GWAS genes in
#' the gene list, GWAS diseases in the ontology, and expression genes in the
#' gene list.
#'
#' \[ESP\] Comprueba la cobertura entre datasets: genes en interactoma, genes
#' GWAS en lista de genes, enfermedades GWAS en ontologia, y genes de expresion
#' en lista de genes.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param genes \[EN\] Validated gene `data.table`.\cr
#'   \[ESP\] `data.table` de genes validado.
#' @param interactome \[EN\] Validated interactome `data.table`.\cr
#'   \[ESP\] `data.table` de interactoma validado.
#' @param gwas_common \[EN\] Validated common GWAS `data.table`.\cr
#'   \[ESP\] `data.table` GWAS comun validado.
#' @param gwas_rare \[EN\] Validated rare GWAS `data.table`.\cr
#'   \[ESP\] `data.table` GWAS raro validado.
#' @param expression \[EN\] Validated expression `data.table`.\cr
#'   \[ESP\] `data.table` de expresion validado.
#' @param diseases \[EN\] Validated disease `data.table`.\cr
#'   \[ESP\] `data.table` de enfermedades validado.
#'
#' @return \[EN\] List with coverage percentages between datasets.\cr
#'   \[ESP\] Lista con porcentajes de cobertura entre datasets.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' cov    <- validate_cross(
#'   config,
#'   read_genes(config),        read_interactome(config),
#'   read_gwas_common(config),  read_gwas_rare(config),
#'   read_expression(config),   read_diseases(config)
#' )
#' }
validate_cross <- function(config, genes, interactome,
                           gwas_common, gwas_rare, expression, diseases) {
  gene_ids    <- genes[["id"]]
  disease_ids <- diseases[["id"]]

  # Genes in interactome
  ppi_genes <- unique(c(interactome[["targetA"]], interactome[["targetB"]]))
  cov_ppi   <- round(100 * sum(gene_ids %in% ppi_genes) / length(gene_ids), 1)

  # Common GWAS: genes and diseases against reference
  cov_gwas_c_genes <- round(
    100 * sum(unique(gwas_common[["targetId"]]) %in% gene_ids) /
      data.table::uniqueN(gwas_common[["targetId"]]), 1
  )
  cov_gwas_c_dis   <- round(
    100 * sum(unique(gwas_common[["diseaseId"]]) %in% disease_ids) /
      data.table::uniqueN(gwas_common[["diseaseId"]]), 1
  )

  # Rare GWAS: genes and diseases against reference
  cov_gwas_r_genes <- round(
    100 * sum(unique(gwas_rare[["targetId"]]) %in% gene_ids) /
      data.table::uniqueN(gwas_rare[["targetId"]]), 1
  )
  cov_gwas_r_dis   <- round(
    100 * sum(unique(gwas_rare[["diseaseId"]]) %in% disease_ids) /
      data.table::uniqueN(gwas_rare[["diseaseId"]]), 1
  )

  # Expression: genes against reference
  cov_expr <- round(
    100 * sum(unique(expression[["id"]]) %in% gene_ids) /
      data.table::uniqueN(expression[["id"]]), 1
  )

  gh_log(config, "INFO", sprintf(
    "[cross] gene coverage in PPI: %s%% | GWAS-common genes: %s%% dis: %s%% | GWAS-rare genes: %s%% dis: %s%% | expression: %s%%",
    cov_ppi, cov_gwas_c_genes, cov_gwas_c_dis,
    cov_gwas_r_genes, cov_gwas_r_dis, cov_expr
  ))

  list(
    ppi_genes_cov        = cov_ppi,
    gwas_comun_genes_cov = cov_gwas_c_genes,
    gwas_comun_dis_cov   = cov_gwas_c_dis,
    gwas_rara_genes_cov  = cov_gwas_r_genes,
    gwas_rara_dis_cov    = cov_gwas_r_dis,
    expresion_genes_cov  = cov_expr
  )
}
