library(tidyverse)
library(fleetbr)
library(roadtrafficdeaths)
library(here)
library(arrow)
library(tidymodels)

load(here("data/pib_mensal.rda"))

url <- "https://github.com/ONSV/prfdata/releases/download/v0.2.1/prf_sinistros.parquet"

temp <- tempfile(fileext = ".parquet")

download.file(url, temp, quiet = T)

prf_sinistros <- open_dataset(temp)

metricas <- metric_set(rmse, mae, rsq)

df_frota_2024 <- fleetbr |> 
  pivot_wider(names_from = modal, values_from = frota) |> 
  mutate(
    data = ym(paste0(ano,"-",mes)),
    automovel = AUTOMOVEL + CAMINHONETE + CAMIONETA + UTILITARIO,
    motocicleta = MOTOCICLETA + CICLOMOTOR + MOTONETA
  ) |> 
  rename(total = TOTAL) |> 
  summarise(
    .by = data,
    veiculos = sum(total),
    automovel = sum(automovel),
    motocicleta = sum(motocicleta)
  )

df_mortes_2024 <- rtdeaths |> 
  mutate(mes = month(data_ocorrencia),
         ano = year(data_ocorrencia),
         data = ym(paste0(ano, "-", mes))) |> 
  count(data, name = "mortes") |> 
  drop_na()

df_pib_2024 <- pib_mensal |> 
  mutate(data = ym(paste0(ano, "-", mes))) |> 
  group_by(data) |> 
  summarise(pib)

df_prf_2024 <- prf_sinistros |> 
  collect() |> 
  mutate(
    acidentes_fatais = if_else(
      classificacao_acidente == "Com Vítimas Fatais", 1, 0, missing = 0
    ),
    mes = month(data_inversa),
    data = ym(paste0(ano, "-", mes))
  ) |> 
  summarise(
    .by = data,
    acidentes = n(),
    acidentes_fatais = sum(acidentes_fatais),
    feridos = sum(feridos),
    mortes_prf = sum(mortos)
  ) |> 
  arrange(data)

dados_mensais_2024 <- 
  list(df_frota_2024, df_mortes_2024, df_pib_2024, df_prf_2024) |> 
  reduce(full_join, by = "data") |> 
  arrange(data)

split_2024 <- initial_split(drop_na(dados_mensais_2024), prop = 0.8)

train_2024 <- training(split_2024)
test_2024 <- testing(split_2024)

rec_mensal_2024 <-
  recipe(train_2024, mortes ~ .) |> 
  remove_role(c(mortes_prf, data), old_role = "predictor") |> 
  step_normalize(all_numeric_predictors())

rf <- 
  rand_forest(
    mode = "regression",
    mtry = 5,
    trees = 5000
  ) |> 
  set_engine("ranger")

rf_wflow <- 
  workflow() |> 
  add_model(rf) |> 
  add_recipe(rec_mensal_2024) |>
  fit(train_2024)

rf_pred <- bind_cols(
  predict(rf_wflow, drop_na(dados_mensais_2024, veiculos)),
  drop_na(dados_mensais_2024, veiculos)
)

metricas_rf <- bind_cols(
  predict(rf_wflow, test_2024),
  test_2024
) |> 
  metricas(truth = mortes, estimate = .pred)