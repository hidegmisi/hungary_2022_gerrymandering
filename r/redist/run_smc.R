# Sequential Monte Carlo redistricting via redist (Slice 6).
# Usage: Rscript run_smc.R <run_dir>
# Expects run_dir/run.json, precincts.gpkg, edges.csv (from hungary_ge.export_redist_bundle).

local({
  argv <- commandArgs(trailingOnly = FALSE)
  f <- grep("^--file=", argv, value = TRUE)
  script_dir <- if (length(f)) {
    dirname(normalizePath(sub("^--file=", "", f[1])))
  } else {
    getwd()
  }
  act <- file.path(script_dir, "renv", "activate.R")
  if (file.exists(act)) {
    source(act)
  }
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1L) {
  stop("usage: Rscript run_smc.R <run_dir>", call. = FALSE)
}

run_dir <- normalizePath(args[1], winslash = "/", mustWork = TRUE)
json_path <- file.path(run_dir, "run.json")
if (!file.exists(json_path)) {
  stop(paste("missing", json_path), call. = FALSE)
}

suppressPackageStartupMessages({
  library(jsonlite)
  library(sf)
  library(redist)
})

meta <- fromJSON(json_path, simplifyVector = TRUE, simplifyDataFrame = FALSE)
if (is.null(meta$schema_version) || meta$schema_version != "hungary_ge.redist_run/v1") {
  warning("unexpected schema_version in run.json", immediate. = TRUE)
}

gpkg <- file.path(run_dir, meta$precinct_gpkg)
precincts <- st_read(gpkg, layer = "precincts", quiet = TRUE)

edges_path <- file.path(run_dir, meta$edges_csv)
edges <- read.csv(edges_path, colClasses = c("integer", "integer"))

n <- as.integer(meta$n_nodes)
adj <- replicate(n, integer(0), simplify = FALSE)
for (k in seq_len(nrow(edges))) {
  ii <- edges$i[k] + 1L
  jj <- edges$j[k] + 1L
  adj[[ii]] <- c(adj[[ii]], jj)
  adj[[jj]] <- c(adj[[jj]], ii)
}
adj <- lapply(adj, function(x) sort(unique(as.integer(x))))

pop_col <- meta$total_pop_column
nd <- as.integer(meta$ndists)
pt <- as.numeric(meta$pop_tol)

map <- redist_map(
  precincts,
  ndists = nd,
  pop_tol = pt,
  total_pop = precincts[[pop_col]],
  adj = adj
)

seed <- meta$seed
if (!is.null(seed) && !is.na(seed)) {
  set.seed(as.integer(seed))
}

nsim <- as.integer(meta$n_sims)
nrun <- as.integer(meta$n_runs)
cmp <- as.numeric(if (!is.null(meta$compactness)) meta$compactness else 1)

extras <- meta$redist_extras
ncores <- 1L
if (is.list(extras) && !is.null(extras$ncores)) {
  ncores <- as.integer(extras$ncores)
}

sims <- redist_smc(
  map,
  nsims = nsim,
  runs = nrun,
  compactness = cmp,
  ncores = ncores,
  silent = TRUE
)

mat <- get_plans_matrix(sims)
nr <- nrow(mat)
nc <- ncol(mat)
if (nr != n) {
  stop(paste("plans matrix rows", nr, "!= n_nodes", n), call. = FALSE)
}

unit_index <- rep(seq_len(nr) - 1L, times = nc)
draw <- rep(seq_len(nc), each = nr)
chain <- ((draw - 1L) %/% nsim) + 1L
district <- as.integer(mat)

out <- data.frame(
  unit_index = unit_index,
  draw = draw,
  chain = chain,
  district = district,
  stringsAsFactors = FALSE
)

out_csv <- file.path(run_dir, meta$assignments_csv)
write.csv(out, out_csv, row.names = FALSE)
