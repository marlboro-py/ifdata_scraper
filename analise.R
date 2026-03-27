# carteira - pequena, media, grande
# pequenas carteiras em crescimento
# carteiras medias em crescimento/queda
# grandes carteiras com alta inadimplencia

# carteira desce c inadimplencia crescendo -> efeito dos bons quitando e ficando so os ruins
# carteira desce c inadimplencia descendo -> rebalanceamento de carteira
# carteira cresce c inadimplencia crescendo -> colocando gente nova pra "dentro", colocando gente de maior risco OU cara que nao esta sabendo conceder (se nao mexeu mix)
# carteira cresce c inadimplencia descendo -> colocando gente nova pra "dentro" e isso acaba reduzindo a inadimplencia
library(tidyverse)
library(here)
options(scipen = 9999999)

con_clean <- DBI::dbConnect(duckdb::duckdb(), here("repos/ifdata_scraper/ifdata_clean.duckdb"))

carteira_modalidade_prazo <- con_clean %>%
                                tbl("carteira_modalidade_prazo") %>%
                                collect() %>%
                                filter(
                                    data >= as.Date("2025-03-01", format = "%Y-%m-%d"),
                                    !is.na(total_da_carteira),
                                    total_da_carteira > 1
                                )

# tirando o prefixo de prudencial pq isso quebra a continuidade da base
carteira_modalidade_prazo <- carteira_modalidade_prazo %>%
                                mutate(
                                    instituicao = str_replace(instituicao, " - PRUDENCIAL", "")
                                )

# calculando crescimento total da carteira
cresc_carteira <- carteira_modalidade_prazo %>%
                    group_by(data, tipo_pessoa, modalidade) %>%
                    mutate( # agrega o mercado por modalidade
                        total_mercado = sum(total, na.rm = TRUE)
                    ) %>%
                    group_by(instituicao, data, tipo_pessoa, modalidade) %>%
                    mutate( # faz calculos da relacao instituicao < > mercado nivel mdl
                        venc_15dias = sum(vencido_a_partir_de_15_dias, na.rm = TRUE),
                        perc_mdl = total / total_da_carteira,
                        npl_15_mdl = venc_15dias / total,
                        mkt_share_mdl = total / total_mercado
                    ) %>%
                    select(
                        instituicao, data, tipo_if, tipo_pessoa, modalidade,
                        total_da_carteira, total_mercado, total, venc_15dias, perc_mdl, npl_15_mdl, mkt_share_mdl
                    ) %>%
                    arrange(instituicao, data, tipo_pessoa, modalidade) %>%
                    group_by(instituicao, tipo_pessoa, modalidade) %>%
                    mutate( # variacao anual por modalidade
                        qoq_mdl = round(
                            (total - lag(total)) / lag(total), 4
                        ) * 100,
                        qoq_inad_mdl = round(
                            (npl_15_mdl - lag(npl_15_mdl)) / lag(npl_15_mdl), 4
                        ) * 100,
                        qoq_mdl_perc = round(
                            (perc_mdl - lag(perc_mdl)) / lag(perc_mdl), 6
                        ) * 100,
                        qoq_mdl_avg = mean(qoq_mdl, na.rm = TRUE),
                        perc_mdl_avg = mean(perc_mdl, na.rm = TRUE),
                        npl_15_mdl_avg = mean(npl_15_mdl, na.rm = TRUE),
                        qoq_inad_mdl_avg = mean(qoq_inad_mdl, na.rm = TRUE),
                        mkt_share_mdl_avg = mean(mkt_share_mdl, na.rm = TRUE)
                    ) %>%
                    group_by(data, tipo_pessoa) %>%
                    mutate( # agregacoes a nivel carteira mercado p/ nao perder no filtro
                        total_mercado_por_pessoa = sum(total, na.rm = TRUE)
                    ) %>%
                    group_by(instituicao, data, tipo_pessoa) %>%
                    mutate( # soma os vencidos por 15 dias por pf/pj pra nao perder no filtro
                        venc_15dias_total = sum(venc_15dias, na.rm = TRUE),
                        npl_15 = venc_15dias_total / total_da_carteira,
                        qoq_mdl_perc_avg = mean(abs(qoq_mdl_avg), na.rm = TRUE)
                    ) %>%
                    select(qoq_mdl_perc_avg) %>%
                    pull(.) %>%
                    quantile(., probs = seq(0, 1, by = .1), na.rm = TRUE)
                    filter( # logica: so pega modalidades relevantes pra carteira
                           # ou modalidades que ganharam crescimento significativo
                        ((perc_mdl_avg > .02) | (perc_mdl <= .02 & qoq_mdl_avg >= .20))
                    ) %>%
                    mutate(
                        rnk_perc = row_number(desc(perc_mdl_avg)),
                        rnk_cresc = row_number(desc(qoq_mdl_avg))
                    ) %>%
                    filter(
                        (rnk_perc <= 3 | rnk_cresc <= 3)
                    ) %>% # limpando a base
                    select(
                        instituicao, data, tipo_pessoa, modalidade,
                        qoq_mdl_avg, perc_mdl_avg, mkt_share_mdl_avg, npl_15_mdl_avg, rnk_perc, rnk_cresc,
                        total, total_mercado_por_pessoa, total_da_carteira, venc_15dias_total, npl_15
                    ) %>%
                    arrange(instituicao, data) %>%
                    group_by(instituicao, tipo_pessoa) %>%
                    mutate(
                        mkt_share = round(
                            total_da_carteira / total_mercado_por_pessoa, 6
                        ) * 100,
                        qoq_inad = round(
                            (npl_15 - lag(npl_15, 4)) / lag(npl_15, 4), 4
                        ) * 100,
                        yoy = round(
                            (total_da_carteira - lag(total_da_carteira, 4)) / lag(total_da_carteira, 4), 4
                        ) * 100,
                        qoq_avg = mean(yoy, na.rm = TRUE),
                        inad_avg = mean(npl_15, na.rm = TRUE),
                        qoq_inad_avg = mean(qoq_inad, na.rm = TRUE),
                        mkt_share_avg = mean(mkt_share, na.rm = TRUE)
                    ) %>%
                    mutate(
                        porte = case_when(
                            mkt_share_avg >= 5 ~ "grande",
                            mkt_share_avg >= 1 ~ "medio",
                            mkt_share_avg >= .1 ~ "pequeno",
                            TRUE ~ "muito_pequeno"
                        ),
                        crescimento_carteira = case_when(
                            qoq_avg  < 0 ~ "queda",
                            qoq_avg < 3 ~ "estagnado",
                            qoq_avg <= 11 ~ "baixo",
                            qoq_avg <= 18 ~ "moderado",
                            qoq_avg <= 30 ~ "alto",
                            qoq_avg > 30 ~ "acelerado"
                        ),
                        crescimento_inad = case_when(
                            qoq_inad_avg < 0 ~ "queda",
                            qoq_inad_avg < 25 ~ "baixo",
                            qoq_inad_avg < 50 ~ "moderado",
                            qoq_inad_avg >= 50 ~ "alto"
                        ),
                        crescimento_mdl = case_when(
                            qoq_mdl_avg <  0 ~ "queda",
                            qoq_mdl_avg < 3 ~ "estagnado",
                            qoq_mdl_avg  <= 6 ~ "baixo",
                            qoq_mdl_avg <= 11 ~ "alto",
                            qoq_mdl_avg > 11 ~ "acelerado"
                        ),
                        rebalanceamento_carteira = case_when(
                            qoq_mdl_perc_avg < 5 ~ "estavel",
                            qoq_mdl_avg <= 10 ~ "moderado",
                            qoq_mdl_avg > 10 ~ "significativo"
                        )
                    ) %>%
                    fi
                    select(
                        instituicao, tipo_pessoa, modalidade,
                        qoq_mdl_avg, perc_mdl_avg, mkt_share_mdl_avg, npl_15_mdl_avg, rnk_perc, rnk_cresc,
                        mkt_share_avg, inad_avg, qoq_avg, qoq_inad_avg, porte, crescimento_carteira, crescimento_inad
                    ) %>%
                    distinct(
                        instituicao, tipo_pessoa, modalidade, .keep_all = TRUE
                    )