library(tidyverse)
library(tidymodels)
library(onsvplot)
library(here)
tidymodels_prefer()
options(scipen = 999)

load(here("data/tabela_total.rda"))

source(here("R/linear_model.R"))

res <- df_total |> 
  lm_model() |> 
  lm_extract(df_total)

prediction <- res$pred

dados2023 <- list(
  "ano" = 2023,
  "qnt_acidentes" = 67638,
  "qnt_acidentes_fatais" = 4846,
  "condutores" = 82613049,
  "veiculos_total" = 117366780,
  "populacao" = 203062512
) |> as.data.frame()

df_2023 <- bind_rows(drop_na(df_total), dados2023)

res2 <- df_total |>
  lm_model() |>
  lm_extract(df_2023)

prediction2 <- res2$pred