library(arrow)
library(tidyverse)

url <- "https://github.com/plmex/prfdata/releases/download/v0.2.2/prf_sinistros.parquet"

temp_file <- tempfile(fileext = ".parquet")

download.file(url, temp_file, quiet = T)


sinistros_prf <- open_dataset(temp_file) |> 
  mutate(
    acidentes_fatais = if_else(
      classificacao_acidente == "Com Vítimas Fatais",
      1, 0, missing = 0
    )
  ) |> 
  summarise(
    .by = ano,
    qnt_acidentes = n(),
    qnt_acidentes_fatais = sum(acidentes_fatais),
    qnt_feridos = sum(feridos),
    qnt_mortos = sum(mortos)
  ) |> 
  arrange(ano) |> 
  filter(ano <= 2023) |> 
  collect()

unlink(temp_file)

save(sinistros_prf, file = "data/sinistros_prf.rda")