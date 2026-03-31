# One-shot: user-writable library for machines where base R library is read-only.
# Usage: Rscript install_deps_user.R
uv <- Sys.getenv("R_LIBS_USER")
if (!nzchar(uv)) {
  v <- paste(R.version$major, strsplit(R.version$minor, ".", fixed = TRUE)[[1L]][1L], sep = ".")
  uv <- normalizePath(
    file.path(Sys.getenv("USERPROFILE"), "Documents", "R", "win-library", v),
    mustWork = FALSE
  )
}
dir.create(uv, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(uv, .libPaths()))
install.packages(
  c("jsonlite", "sf", "redist"),
  lib = uv,
  repos = "https://cloud.r-project.org"
)
