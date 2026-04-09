# ============================================================
#  CAPA 2 — DATA  |  data_validate.R
#  Validacion de tipos, rangos, NAs e integridad entre datasets
# ============================================================

# ── helpers internos ──────────────────────────────────────────

#' @noRd
.vcols <- function(config, dt, required, dataset) {
  missing <- setdiff(required, names(dt))
  if (length(missing) > 0L) {
    stop(sprintf(
      "[%s] Columnas requeridas no encontradas: %s",
      dataset, paste(missing, collapse = ", ")
    ))
  }
  invisible(TRUE)
}

#' @noRd
.vna <- function(dt, col, dataset) {
  n <- sum(is.na(dt[[col]]))
  if (n > 0L) {
    gh_warn(sprintf("[%s] %d NAs en columna clave '%s'", dataset, n, col))
  }
  invisible(n)
}

#' @noRd
.vpattern <- function(dt, col, pattern, dataset) {
  n_bad <- sum(!grepl(pattern, dt[[col]], perl = TRUE), na.rm = TRUE)
  if (n_bad > 0L) {
    gh_warn(sprintf(
      "[%s] %d valores en '%s' no coinciden con patron esperado",
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
      "[%s] %d valores en '%s' fuera del rango [%s, %s]",
      dataset, n_bad, col, min_val, max_val
    ))
  }
  invisible(n_bad)
}

# ── funciones publicas ────────────────────────────────────────

#' Validar dataset de genes
#'
#' Comprueba columnas, patron Ensembl, biotipos y duplicados.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt `data.table` devuelto por [read_genes()].
#'
#' @return `dt` sin modificaciones (los problemas se registran via log).
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

  .vna(dt, "id",            "genes")
  .vna(dt, "approvedSymbol","genes")
  .vpattern(dt, "id", "^ENSG\\d+", "genes")

  n_dup <- nrow(dt) - data.table::uniqueN(dt[["id"]])
  if (n_dup > 0L) {
    gh_warn(sprintf("[genes] %d IDs duplicados", n_dup))
  }

  n_pc <- sum(dt[["biotype"]] == "protein_coding", na.rm = TRUE)
  gh_log(config, "INFO", sprintf(
    "[genes] %d filas | %d protein_coding | %d IDs unicos",
    nrow(dt), n_pc, data.table::uniqueN(dt[["id"]])
  ))
  dt
}

#' Validar dataset de interactoma
#'
#' Comprueba columnas, patron Ensembl en targetA/B y rango del score.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt `data.table` devuelto por [read_interactoma()].
#'
#' @return `dt` sin modificaciones.
#'
#' @export
#' @importFrom data.table uniqueN
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' ppi    <- validate_interactoma(config, read_interactoma(config))
#' }
validate_interactoma <- function(config, dt) {
  required <- c("sourceDatabase", "targetA", "intA", "targetB", "intB", "count", "scoring")
  .vcols(config, dt, required, "interactoma")

  .vna(dt, "targetA", "interactoma")
  .vna(dt, "targetB", "interactoma")
  .vpattern(dt, "targetA", "^ENSG\\d+", "interactoma")
  .vpattern(dt, "targetB", "^ENSG\\d+", "interactoma")
  .vrange(dt, "scoring", 0, 1, "interactoma")

  n_genes <- data.table::uniqueN(c(dt[["targetA"]], dt[["targetB"]]))
  gh_log(config, "INFO", sprintf(
    "[interactoma] %d interacciones | %d genes unicos | fuentes: %s",
    nrow(dt), n_genes,
    paste(unique(dt[["sourceDatabase"]]), collapse = ", ")
  ))
  dt
}

#' Validar dataset GWAS variantes comunes
#'
#' Comprueba columnas, patron Ensembl, rango de score y cobertura de enfermedades.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt `data.table` devuelto por [read_gwas_common()].
#'
#' @return `dt` sin modificaciones.
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
    "[gwas_comun] %d asociaciones | %d genes | %d enfermedades | fuentes: %s",
    nrow(dt),
    data.table::uniqueN(dt[["targetId"]]),
    data.table::uniqueN(dt[["diseaseId"]]),
    paste(unique(dt[["datasourceId"]]), collapse = ", ")
  ))
  dt
}

#' Validar dataset GWAS variantes raras
#'
#' Comprueba columnas, patron Ensembl, rango de score y significancia clinica.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt `data.table` devuelto por [read_gwas_rare()].
#'
#' @return `dt` sin modificaciones.
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
    "[gwas_rara] %d asociaciones | %d genes | %d enfermedades | fuentes: %s",
    nrow(dt),
    data.table::uniqueN(dt[["targetId"]]),
    data.table::uniqueN(dt[["diseaseId"]]),
    paste(unique(dt[["datasourceId"]]), collapse = ", ")
  ))
  dt
}

#' Validar dataset de expresion genica
#'
#' Comprueba columnas, patron Ensembl, valores de rna_value y rna_level.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt `data.table` devuelto por [read_expression()].
#'
#' @return `dt` sin modificaciones.
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
    gh_warn(sprintf("[expresion] %d valores de rna_value negativos", n_neg))
  }

  valid_levels <- c(-1L, 0L, 1L, 2L, 3L)
  n_bad_lvl <- sum(!dt[["rna_level"]] %in% valid_levels, na.rm = TRUE)
  if (n_bad_lvl > 0L) {
    gh_warn(sprintf(
      "[expresion] %d valores de rna_level fuera del rango esperado [-1, 3]", n_bad_lvl
    ))
  }

  gh_log(config, "INFO", sprintf(
    "[expresion] %d filas | %d genes | %d tejidos",
    nrow(dt),
    data.table::uniqueN(dt[["id"]]),
    data.table::uniqueN(dt[["efo_code"]])
  ))
  dt
}

#' Validar ontologia de enfermedades
#'
#' Comprueba columnas y que id y name no tengan NAs.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt `data.table` devuelto por [read_diseases()].
#'
#' @return `dt` sin modificaciones.
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
    "[enfermedades] %d terminos ontologicos", nrow(dt)
  ))
  dt
}

#' Validar integridad cruzada entre datasets
#'
#' Comprueba la cobertura entre datasets: genes en interactoma,
#' genes GWAS en lista de genes, enfermedades GWAS en ontologia,
#' y genes de expresion en lista de genes.
#'
#' @param config    Lista de configuracion creada por [load_config()].
#' @param genes     `data.table` de genes validado.
#' @param interactoma `data.table` de interactoma validado.
#' @param gwas_comun  `data.table` GWAS comun validado.
#' @param gwas_rara   `data.table` GWAS raro validado.
#' @param expresion   `data.table` de expresion validado.
#' @param enfermedades `data.table` de enfermedades validado.
#'
#' @return Lista con porcentajes de cobertura entre datasets.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' cov    <- validate_cross(
#'   config,
#'   read_genes(config),        read_interactoma(config),
#'   read_gwas_common(config),  read_gwas_rare(config),
#'   read_expression(config),   read_diseases(config)
#' )
#' }
validate_cross <- function(config, genes, interactoma,
                           gwas_comun, gwas_rara, expresion, enfermedades) {
  gene_ids    <- genes[["id"]]
  disease_ids <- enfermedades[["id"]]

  # Genes en interactoma
  ppi_genes <- unique(c(interactoma[["targetA"]], interactoma[["targetB"]]))
  cov_ppi   <- round(100 * sum(gene_ids %in% ppi_genes) / length(gene_ids), 1)

  # GWAS comun: genes y enfermedades en referencia
  cov_gwas_c_genes <- round(
    100 * sum(unique(gwas_comun[["targetId"]]) %in% gene_ids) /
      data.table::uniqueN(gwas_comun[["targetId"]]), 1
  )
  cov_gwas_c_dis   <- round(
    100 * sum(unique(gwas_comun[["diseaseId"]]) %in% disease_ids) /
      data.table::uniqueN(gwas_comun[["diseaseId"]]), 1
  )

  # GWAS rara: genes y enfermedades en referencia
  cov_gwas_r_genes <- round(
    100 * sum(unique(gwas_rara[["targetId"]]) %in% gene_ids) /
      data.table::uniqueN(gwas_rara[["targetId"]]), 1
  )
  cov_gwas_r_dis   <- round(
    100 * sum(unique(gwas_rara[["diseaseId"]]) %in% disease_ids) /
      data.table::uniqueN(gwas_rara[["diseaseId"]]), 1
  )

  # Expresion: genes en referencia
  cov_expr <- round(
    100 * sum(unique(expresion[["id"]]) %in% gene_ids) /
      data.table::uniqueN(expresion[["id"]]), 1
  )

  gh_log(config, "INFO", sprintf(
    "[cross] cobertura genes en PPI: %s%% | GWAS-comun genes: %s%% dis: %s%% | GWAS-rara genes: %s%% dis: %s%% | expresion: %s%%",
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
