library(tidyverse)
library(arrow)

url <- "https://github.com/ONSV/prfdata/releases/download/v0.2.1/prf_sinistros.parquet"

temp_file <- tempfile(fileext = ".parquet")

download.file(url, temp_file, quiet = T)


sinistros_prf_mensal <- open_dataset(temp_file) |> 
  mutate(
    mes = month(data_inversa),
    acidentes_fatais = if_else(
      classificacao_acidente == "Com Vítimas Fatais",
      1, 0, missing = 0
    )
  ) |> 
  summarise(
    .by = c(mes, ano, uf),
    acidentes = n(),
    acidentes_fatais = sum(acidentes_fatais),
    feridos = sum(feridos),
    mortes = sum(mortos)
  ) |> 
  arrange(mes, ano) |> 
  collect()

unlink(temp_file)

save(sinistros_prf_mensal, file = "data/sinistros_prf_mensal.rda")