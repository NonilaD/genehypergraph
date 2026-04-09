# ============================================================
#  CAPA 2 — DATA  |  data_harmonize.R
#  Filtrado, normalizacion y preparacion para la capa de red
# ============================================================

#' Filtrar genes protein-coding
#'
#' Retiene unicamente genes con `biotype == "protein_coding"` y
#' elimina duplicados por `id`.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt `data.table` validado de genes.
#'
#' @return `data.table` filtrado con genes protein-coding unicos.
#'
#' @export
#' @importFrom data.table copy setkey
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' genes  <- harmonize_genes(config, validate_genes(config, read_genes(config)))
#' }
harmonize_genes <- function(config, dt) {
  out <- data.table::copy(dt)
  out <- out[out[["biotype"]] == "protein_coding"]
  out <- unique(out, by = "id")
  data.table::setkey(out, "id")

  gh_log(config, "INFO", sprintf(
    "[harmonize] genes protein-coding: %d", nrow(out)
  ))
  out
}

#' Filtrar y normalizar interactoma
#'
#' Filtra por score minimo (config), retiene solo interacciones
#' entre genes human-human (ambos targetA y targetB en lista de genes),
#' y elimina auto-interacciones y duplicados.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt     `data.table` validado de interactoma.
#' @param genes  `data.table` armonizado de genes (con clave en `id`).
#'
#' @return `data.table` filtrado y sin duplicados.
#'
#' @export
#' @importFrom data.table copy
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' genes  <- harmonize_genes(config, validate_genes(config, read_genes(config)))
#' ppi    <- harmonize_interactoma(
#'   config,
#'   validate_interactoma(config, read_interactoma(config)),
#'   genes
#' )
#' }
harmonize_interactoma <- function(config, dt, genes) {
  score_min <- get_config(config, "filtros.interactoma.score_min")
  gene_ids  <- genes[["id"]]

  out <- data.table::copy(dt)
  out <- out[out[["scoring"]] >= score_min]
  out <- out[out[["targetA"]] %in% gene_ids & out[["targetB"]] %in% gene_ids]
  out <- out[out[["targetA"]] != out[["targetB"]]]
  out <- unique(out, by = c("targetA", "targetB", "sourceDatabase"))

  gh_log(config, "INFO", sprintf(
    "[harmonize] interactoma: %d interacciones (score >= %s)",
    nrow(out), score_min
  ))
  out
}

#' Filtrar y normalizar GWAS variantes comunes
#'
#' Filtra por score minimo, retiene solo genes en lista de referencia
#' y elimina filas sin diseaseId.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt     `data.table` validado de gwas_comun.
#' @param genes  `data.table` armonizado de genes.
#'
#' @return `data.table` filtrado.
#'
#' @export
#' @importFrom data.table copy
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' genes  <- harmonize_genes(config, validate_genes(config, read_genes(config)))
#' gwas   <- harmonize_gwas_common(
#'   config,
#'   validate_gwas_common(config, read_gwas_common(config)),
#'   genes
#' )
#' }
harmonize_gwas_common <- function(config, dt, genes) {
  score_min <- get_config(config, "filtros.gwas_comun.score_min")
  gene_ids  <- genes[["id"]]

  out <- data.table::copy(dt)
  out <- out[!is.na(out[["diseaseId"]])]
  out <- out[out[["score"]] >= score_min]
  out <- out[out[["targetId"]] %in% gene_ids]

  gh_log(config, "INFO", sprintf(
    "[harmonize] gwas_comun: %d asociaciones (score >= %s)",
    nrow(out), score_min
  ))
  out
}

#' Filtrar y normalizar GWAS variantes raras
#'
#' Filtra por score minimo, por significancia clinica configurada,
#' retiene solo genes en lista de referencia y elimina filas sin diseaseId.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt     `data.table` validado de gwas_rara.
#' @param genes  `data.table` armonizado de genes.
#'
#' @return `data.table` filtrado.
#'
#' @export
#' @importFrom data.table copy
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' genes  <- harmonize_genes(config, validate_genes(config, read_genes(config)))
#' rara   <- harmonize_gwas_rare(
#'   config,
#'   validate_gwas_rare(config, read_gwas_rare(config)),
#'   genes
#' )
#' }
harmonize_gwas_rare <- function(config, dt, genes) {
  score_min <- get_config(config, "filtros.gwas_rara.score_min")
  gene_ids  <- genes[["id"]]

  out <- data.table::copy(dt)
  out <- out[!is.na(out[["diseaseId"]])]
  out <- out[out[["score"]] >= score_min]
  out <- out[out[["targetId"]] %in% gene_ids]

  sig_excluir <- get_config(config, "filtros.gwas_rara.clinicalSignificances_excluir")
  if (!is.null(sig_excluir) && length(sig_excluir) > 0L) {
    pattern <- paste(sig_excluir, collapse = "|")
    out <- out[!grepl(pattern, out[["clinicalSignificances"]], ignore.case = TRUE) |
                 is.na(out[["clinicalSignificances"]])]
  }

  gh_log(config, "INFO", sprintf(
    "[harmonize] gwas_rara: %d asociaciones (score >= %s)",
    nrow(out), score_min
  ))
  out
}

#' Filtrar y normalizar datos de expresion genica
#'
#' Filtra por nivel de expresion minimo (rna_level) y por genes
#' en lista de referencia.
#'
#' @param config Lista de configuracion creada por [load_config()].
#' @param dt     `data.table` validado de expresion.
#' @param genes  `data.table` armonizado de genes.
#'
#' @return `data.table` filtrado.
#'
#' @export
#' @importFrom data.table copy
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' genes  <- harmonize_genes(config, validate_genes(config, read_genes(config)))
#' expr   <- harmonize_expression(
#'   config,
#'   validate_expression(config, read_expression(config)),
#'   genes
#' )
#' }
harmonize_expression <- function(config, dt, genes) {
  level_min <- get_config(config, "filtros.expresion.rna_level_min")
  gene_ids  <- genes[["id"]]

  out <- data.table::copy(dt)
  out <- out[out[["rna_level"]] >= level_min]
  out <- out[out[["id"]] %in% gene_ids]

  gh_log(config, "INFO", sprintf(
    "[harmonize] expresion: %d filas (rna_level >= %d)",
    nrow(out), level_min
  ))
  out
}

#' Filtrar ontologia de enfermedades
#'
#' Retiene solo enfermedades presentes en al menos uno de los
#' datasets GWAS (comun o raro) para reducir la ontologia
#' al subconjunto relevante.
#'
#' @param config       Lista de configuracion creada por [load_config()].
#' @param dt           `data.table` validado de enfermedades.
#' @param gwas_comun   `data.table` armonizado de gwas_comun.
#' @param gwas_rara    `data.table` armonizado de gwas_rara.
#'
#' @return `data.table` filtrado de enfermedades.
#'
#' @export
#' @importFrom data.table copy
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' genes  <- harmonize_genes(config, validate_genes(config, read_genes(config)))
#' gwas_c <- validate_gwas_common(config, read_gwas_common(config))
#' gwas_r <- validate_gwas_rare(config, read_gwas_rare(config))
#' gc     <- harmonize_gwas_common(config, gwas_c, genes)
#' gr     <- harmonize_gwas_rare(config, gwas_r, genes)
#' dis    <- harmonize_diseases(
#'   config,
#'   validate_diseases(config, read_diseases(config)),
#'   gc, gr
#' )
#' }
harmonize_diseases <- function(config, dt, gwas_comun, gwas_rara) {
  dis_gwas <- unique(c(gwas_comun[["diseaseId"]], gwas_rara[["diseaseId"]]))
  out      <- data.table::copy(dt)
  out      <- out[out[["id"]] %in% dis_gwas]

  gh_log(config, "INFO", sprintf(
    "[harmonize] enfermedades: %d terminos relevantes", nrow(out)
  ))
  out
}
