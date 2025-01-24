library(tidymodels)
library(tidyverse)
library(fleetbr)
library(arrow)
library(here)

load(here("data","tabela_total.rda"))
load(here("data","tabela_total_mensal.rda"))
load(here("data","tabela_condutores.rda"))
load(here("data/pib_mensal.rda"))

url <- "https://github.com/ONSV/prfdata/releases/download/v0.2.1/prf_sinistros.parquet"
temp <- tempfile(fileext = ".parquet")
download.file(url, temp, quiet = T)
prf_sinistros <- open_dataset(temp)

metricas <- metric_set(rmse, mae, rsq)

# anual

dados_modelo_2024 <- list(
  drop_na(count(rename(rtdeaths, ano = ano_ocorrencia), ano, name = "mortes")),
  summarise(filter(fleetbr, modal == "TOTAL", mes == 7), 
            .by = ano, frota = sum(frota)),
  prf_sinistros |> 
    filter(classificacao_acidente == "Com Vítimas Fatais") |> 
    count(ano, name = "acids_fatais") |> 
    collect(),
  prf_sinistros |> 
    count(ano, name = "acids") |> 
    collect(),
  tabela_condutores
) |> 
  reduce(full_join, by = "ano") |> 
  arrange(ano)

rec_anual_2024 <-
  recipe(x = drop_na(dados_modelo_2024), mortes ~ .) |> 
  remove_role(ano, old_role = "predictor") |> 
  step_normalize(all_numeric_predictors())

modelo_anual_2024 <-
  linear_reg() |> 
  set_engine("lm")

wflow_anual_2024 <-
  workflow() |> 
  add_model(modelo_anual_2024) |> 
  add_recipe(rec_anual_2024) |> 
  fit(drop_na(dados_modelo_2024))

pred_anual_2024 <- bind_cols(
  dados_modelo_2024,
  predict(wflow_anual_2024, dados_modelo_2024),
  predict(wflow_anual_2024, dados_modelo_2024, type = "conf_int")
)

metricas_anual <- metricas(data = pred_anual_2024, truth = mortes, estimate = .pred)

# trimestral

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

df_trimestre_2024 <- dados_mensais_2024 |>
  mutate(
    trimestre = quarter(data),
    data = quarter(data, type = "date_last")
  ) |> 
  group_by(data, trimestre) |> 
  summarise(
    veiculos = last(veiculos),
    automovel = last(automovel),
    motocicleta = last(motocicleta),
    mortes = sum(mortes),
    pib = sum(pib),
    acidentes = sum(acidentes),
    acidentes_fatais = sum(acidentes_fatais),
    feridos = sum(feridos),
    mortes_prf = sum(mortes_prf)
  ) |> 
  ungroup()

splits_trimestre <- initial_split(drop_na(df_trimestre_2024), prop = 3/4)
train_trimestre <- training(splits_trimestre)
test_trimestre <- testing(splits_trimestre)

rec <- 
  recipe(df_trimestre_2024, mortes ~ .) |> 
  remove_role(c(mortes_prf, trimestre, data), old_role = "predictor") |> 
  step_normalize(all_numeric_predictors())

model <-
  linear_reg() |>
  set_engine("lm")

lm_wflow <-
  workflow() |> 
  add_model(model) |> 
  add_recipe(rec)

lm_wflow_fit <-
  lm_wflow |> 
  fit(train_trimestre)

pred_trimestre_2024 <- bind_cols(
  df_trimestre_2024,
  predict(lm_wflow_fit, df_trimestre_2024),
  predict(lm_wflow_fit, df_trimestre_2024, type = "conf_int")
)

metricas_trimestre <- bind_cols(
  test_trimestre,
  predict(lm_wflow_fit, test_trimestre)
) |> 
  metricas(truth = mortes, estimate = .pred)

# mensal

split_2024 <- initial_split(drop_na(dados_mensais_2024), prop = 0.8)

train_2024 <- training(split_2024)
test_2024 <- testing(split_2024)

rec_mensal_2024 <-
  recipe(train_2024, mortes ~ .) |> 
  remove_role(c(mortes_prf, data), old_role = "predictor") |> 
  step_normalize(all_numeric_predictors())

lm_mensal_2024 <-
  linear_reg() |> 
  set_engine("lm")

lm_wflow_mensal_2024 <-
  workflow() |> 
  add_model(lm_mensal_2024) |> 
  add_recipe(rec_mensal_2024) |> 
  fit(train_2024)

pred_mensal_2024 <-
  bind_cols(
    dados_mensais_2024,
    predict(lm_wflow_mensal_2024, dados_mensais_2024),
    predict(lm_wflow_mensal_2024, dados_mensais_2024, type = "conf_int")
  )

metricas_mensal <- metricas(pred_mensal_2024, truth = mortes, estimate = .pred)