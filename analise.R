# carteira - pequena, media, grande
# pequenas carteiras em crescimento
# carteiras medias em crescimento/queda
# grandes carteiras com alta inadimplencia

# carteira desce c inadimplencia crescendo -> efeito dos bons quitando e ficando so os ruins
# carteira desce c inadimplencia descendo -> rebalanceamento de carteira
# carteira cresce c inadimplencia crescendo -> colocando gente nova pra "dentro", colocando gente de maior risco OU cara que nao esta sabendo conceder (se nao mexeu mix)
# carteira cresce c inadimplencia descendo -> colocando gente nova pra "dentro" e isso acaba reduzindo a inadimplencia

## mudanca de prazos -> migracao de mix de a vencer
library(tidyverse)
library(here)
options(scipen = 9999999)

con_clean <- DBI::dbConnect(duckdb::duckdb(), here("repos/ifdata_scraper/ifdata_clean.duckdb"))

carteira_modalidade_prazo <- con_clean %>%
                                tbl("carteira_modalidade_prazo") %>%
                                collect() %>%
                                filter(data >= as.Date("2022-01-01"), !is.na(total_da_carteira), total_da_carteira > 1)

# calculando crescimento total da carteira
cresc_carteira <- carteira_modalidade_prazo %>%
                    group_by(instituicao, tipo_pessoa, data) %>%
                    mutate(
                        vencido_15dias = sum(vencido_a_partir_de_15_dias, na.rm = TRUE),
                        npl_15 = round(vencido_15dias / total_da_carteira, 4) * 100
                    ) %>%
                    distinct(data, instituicao, tipo_pessoa, total_da_carteira, vencido_15dias, npl_15) %>%
                    group_by(data, tipo_pessoa) %>%
                    mutate(
                        carteira_mercado = sum(total_da_carteira)
                    ) %>%
                    arrange(instituicao, data) %>%
                    group_by(instituicao, tipo_pessoa) %>%
                    mutate(
                        participacao_mercado = round(
                            total_da_carteira / carteira_mercado, 6
                        ) * 100,
                        yoy_inad = round(
                            (npl_15 - lag(npl_15, 4)) / lag(npl_15, 4), 4
                        ) * 100,
                        yoy = round(
                            (total_da_carteira - lag(total_da_carteira, 4)) / lag(total_da_carteira, 4), 4
                        ) * 100,
                        yoy_avg = mean(yoy, na.rm = TRUE),
                        inad_avg = mean(npl_15, na.rm = TRUE),
                        yoy_inad_avg = mean(yoy_inad, na.rm = TRUE)
                    ) %>%
                    ungroup() %>%
                    mutate(
                        porte = case_when(
                            participacao_mercado >= 5 ~ "grande",
                            participacao_mercado >= 1 ~ "medio",
                            participacao_mercado >= .1 ~ "pequeno",
                            TRUE ~ "muito_pequeno"
                        ),
                        crescimento = case_when(
                            yoy_avg  < 0 ~ "queda",
                            yoy_avg < 3 ~ "estagnado",
                            yoy_avg <= 11 ~ "baixo",
                            yoy_avg <= 18 ~ "moderado",
                            yoy_avg <= 30 ~ "alto",
                            yoy_avg > 30 ~ "acelerado"
                        ),
                        inad = case_when(
                            inad_avg < .5 ~ "muito_baixa",
                            inad_avg < 1.5 ~ "baixa",
                            inad_avg < 3 ~ "media",
                            inad_avg >= 3 ~ "alta"
                        ),
                        crescimento_inad = case_when(
                            yoy_inad_avg < 0 ~ "queda",
                            yoy_inad_avg < 25 ~ "baixo",
                            yoy_inad_avg < 50 ~ "moderado",
                            yoy_inad_avg >= 50 ~ "alto"
                        )
                    )
