# ============================================================
#  LAYER 2 — DATA  |  data_harmonize.R
#  Filtering, normalisation and preparation for the network layer
# ============================================================

#' Filter protein-coding genes
#'
#' \[EN\] Retains only genes with `biotype == "protein_coding"` and removes
#' duplicates by `id`.
#'
#' \[ESP\] Retiene unicamente genes con `biotype == "protein_coding"` y elimina
#' duplicados por `id`.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] Validated gene `data.table`.\cr
#'   \[ESP\] `data.table` validado de genes.
#'
#' @return \[EN\] Filtered `data.table` with unique protein-coding genes.\cr
#'   \[ESP\] `data.table` filtrado con genes protein-coding unicos.
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
    "[harmonize] protein-coding genes: %d", nrow(out)
  ))
  out
}

#' Filter and normalise interactome
#'
#' \[EN\] Filters by minimum score (config), retains only human-human
#' interactions (both targetA and targetB in the gene list), and removes
#' self-interactions and duplicates.
#'
#' \[ESP\] Filtra por score minimo (config), retiene solo interacciones
#' entre genes human-human (ambos targetA y targetB en lista de genes), y
#' elimina auto-interacciones y duplicados.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] Validated interactome `data.table`.\cr
#'   \[ESP\] `data.table` validado de interactoma.
#' @param genes \[EN\] Harmonised gene `data.table` (keyed on `id`).\cr
#'   \[ESP\] `data.table` armonizado de genes (con clave en `id`).
#'
#' @return \[EN\] Filtered `data.table` without duplicates.\cr
#'   \[ESP\] `data.table` filtrado y sin duplicados.
#'
#' @export
#' @importFrom data.table copy
#'
#' @examples
#' \dontrun{
#' config <- load_config()
#' genes  <- harmonize_genes(config, validate_genes(config, read_genes(config)))
#' ppi    <- harmonize_interactome(
#'   config,
#'   validate_interactome(config, read_interactome(config)),
#'   genes
#' )
#' }
harmonize_interactome <- function(config, dt, genes) {
  score_min <- get_config(config, "filtros.interactoma.score_min")
  gene_ids  <- genes[["id"]]

  out <- data.table::copy(dt)
  out <- out[out[["scoring"]] >= score_min]
  out <- out[out[["targetA"]] %in% gene_ids & out[["targetB"]] %in% gene_ids]
  out <- out[out[["targetA"]] != out[["targetB"]]]
  out <- unique(out, by = c("targetA", "targetB", "sourceDatabase"))

  gh_log(config, "INFO", sprintf(
    "[harmonize] interactome: %d interactions (score >= %s)",
    nrow(out), score_min
  ))
  out
}

#' Filter and normalise common variant GWAS
#'
#' \[EN\] Filters by minimum score, retains only genes in the reference list
#' and removes rows without diseaseId.
#'
#' \[ESP\] Filtra por score minimo, retiene solo genes en lista de referencia y
#' elimina filas sin diseaseId.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] Validated gwas_comun `data.table`.\cr
#'   \[ESP\] `data.table` validado de gwas_comun.
#' @param genes \[EN\] Harmonised gene `data.table`.\cr
#'   \[ESP\] `data.table` armonizado de genes.
#'
#' @return \[EN\] Filtered `data.table`.\cr
#'   \[ESP\] `data.table` filtrado.
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
    "[harmonize] gwas_comun: %d associations (score >= %s)",
    nrow(out), score_min
  ))
  out
}

#' Filter and normalise rare variant GWAS
#'
#' \[EN\] Filters by minimum score, by configured clinical significance,
#' retains only genes in the reference list and removes rows without diseaseId.
#'
#' \[ESP\] Filtra por score minimo, por significancia clinica configurada,
#' retiene solo genes en lista de referencia y elimina filas sin diseaseId.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] Validated gwas_rara `data.table`.\cr
#'   \[ESP\] `data.table` validado de gwas_rara.
#' @param genes \[EN\] Harmonised gene `data.table`.\cr
#'   \[ESP\] `data.table` armonizado de genes.
#'
#' @return \[EN\] Filtered `data.table`.\cr
#'   \[ESP\] `data.table` filtrado.
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

  excl_sigs <- get_config(config, "filtros.gwas_rara.clinicalSignificances_excluir")
  if (!is.null(excl_sigs) && length(excl_sigs) > 0L) {
    pattern <- paste(excl_sigs, collapse = "|")
    out <- out[!grepl(pattern, out[["clinicalSignificances"]], ignore.case = TRUE) |
                 is.na(out[["clinicalSignificances"]])]
  }

  gh_log(config, "INFO", sprintf(
    "[harmonize] gwas_rara: %d associations (score >= %s)",
    nrow(out), score_min
  ))
  out
}

#' Filter and normalise gene expression data
#'
#' \[EN\] Filters by minimum expression level (rna_level) and by genes in the
#' reference list.
#'
#' \[ESP\] Filtra por nivel de expresion minimo (rna_level) y por genes en
#' lista de referencia.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] Validated expression `data.table`.\cr
#'   \[ESP\] `data.table` validado de expresion.
#' @param genes \[EN\] Harmonised gene `data.table`.\cr
#'   \[ESP\] `data.table` armonizado de genes.
#'
#' @return \[EN\] Filtered `data.table`.\cr
#'   \[ESP\] `data.table` filtrado.
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
    "[harmonize] expression: %d rows (rna_level >= %d)",
    nrow(out), level_min
  ))
  out
}

#' Filter disease ontology
#'
#' \[EN\] Retains only diseases present in at least one of the GWAS datasets
#' (common or rare) to reduce the ontology to the relevant subset.
#'
#' \[ESP\] Retiene solo enfermedades presentes en al menos uno de los datasets
#' GWAS (comun o raro) para reducir la ontologia al subconjunto relevante.
#'
#' @param config \[EN\] Configuration list created by [load_config()].\cr
#'   \[ESP\] Lista de configuracion creada por [load_config()].
#' @param dt \[EN\] Validated disease `data.table`.\cr
#'   \[ESP\] `data.table` validado de enfermedades.
#' @param gwas_common \[EN\] Harmonised gwas_comun `data.table`.\cr
#'   \[ESP\] `data.table` armonizado de gwas_comun.
#' @param gwas_rare \[EN\] Harmonised gwas_rara `data.table`.\cr
#'   \[ESP\] `data.table` armonizado de gwas_rara.
#'
#' @return \[EN\] Filtered disease `data.table`.\cr
#'   \[ESP\] `data.table` filtrado de enfermedades.
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
harmonize_diseases <- function(config, dt, gwas_common, gwas_rare) {
  gwas_disease_ids <- unique(c(gwas_common[["diseaseId"]], gwas_rare[["diseaseId"]]))
  out              <- data.table::copy(dt)
  out              <- out[out[["id"]] %in% gwas_disease_ids]

  gh_log(config, "INFO", sprintf(
    "[harmonize] diseases: %d relevant terms", nrow(out)
  ))
  out
}
