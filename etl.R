library(tidyverse)
library(here)

con <- DBI::dbConnect(duckdb::duckdb(), here("repos/ifdata_scraper/ifdata_raw.duckdb"))
con_clean <- DBI::dbConnect(duckdb::duckdb(), here("repos/ifdata_scraper/ifdata_clean.duckdb"))

##### ATIVO ######
ativo <- con %>%
            tbl("ativo") %>%
            collect() %>%
            janitor::clean_names()

# retrofitando algumas colunas
ativo <- ativo %>%
            mutate(
                instituicao = coalesce(instituicao, ativos_instituicoes, instituicao_financeira),
                tc = coalesce(tc, ativos_tc),
                td = coalesce(td, ativos_td),
                data = coalesce(data, ativos_data_balancete),
                disponibilidades_a = coalesce(disponibilidades_a, ativos_disponibilidades),
                aplicacoes_interfinanceiras_de_liquidez_b = coalesce(
                    aplicacoes_interfinanceiras_de_liquidez_b, ativos_aplicacoes_interfinanceiras
                ),
                tvm_e_instrumentos_financeiros_derivativos_c = coalesce(
                    tvm_e_instrumentos_financeiros_derivativos_c, ativos_tvm_e_instrumentos_financeiros_derivativos
                ),
                ativo_total_k_a_b_c_d_e_f_g_h_i_j = coalesce(
                    ativo_total_k_a_b_c_d_e_f_g_h_i_j, ativo_total_k_i_j, ativos_ativo_total
                ),
                ativo_permanente_j = coalesce(
                    ativo_permanente_j, ativos_permanente
                )
            ) %>%
            select(
                -c(
                    ativos_instituicoes, ativos_data_balancete, ativos_disponibilidades,
                    ativos_aplicacoes_interfinanceiras, ativos_tvm_e_instrumentos_financeiros_derivativos,
                    ativo_total_k_i_j, ativos_ativo_total, instituicao_financeira, ativos_permanente,
                    ativos_tc, ativos_td
                )
            ) %>%
            rename(
                ativo_total = ativo_total_k_a_b_c_d_e_f_g_h_i_j
            )

# transformando colunas
ativo <- ativo %>%
            select(
                instituicao, data, tipo_if, arquivo_origem, codigo, conglomerado, conglomerado_financeiro,
                conglomerado_financeiro_2, conglomerado_prudencial, conglomerado_prudencial_2, ativos_ranking,
                ativos_obs, tcb, tc, ti, td, sr, cidade, uf, everything()
            ) %>%
            mutate(
                data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                across(
                    disponibilidades_a:ativos_imobilizado_de_arrendamento, ~ as.integer(str_replace_all(.x, "\\.", ""))
                )
            )

# datas NAs sao consolidados dos arquivos - da pra filtrar fora
ativo <- ativo %>%
            filter(!is.na(data))

DBI::dbWriteTable(con_clean, "ativo", ativo, overwrite = TRUE)

rm(ativo)

### CARTEIRAS DE CREDITO - MODALIDADE E PRAZO ###
carteira_pf_modalidade_prazo <- con %>%
                                    tbl("carteira_de_credito_ativa_pessoa_fisica_modalidade_e_prazo_de_vencimento") %>%
                                    collect()

carteira_pj_modalidade_prazo <- con %>%
                                    tbl("carteira_de_credito_ativa_pessoa_juridica_modalidade_e_prazo_de_vencimento") %>% #nolint
                                    collect()

# transformando em formato longo
carteira_pf_modalidade_prazo <- carteira_pf_modalidade_prazo %>%
                                    select(
                                        instituicao, data, tipo_if, arquivo_origem, codigo,
                                        segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                                        uf, (starts_with("total") | ends_with("total")), everything()
                                    ) %>%
                                    rename(
                                        exterior_pessoa_fisica_total = total_exterior_pessoa_fisica
                                    ) %>%
                                    pivot_longer(
                                        exterior_pessoa_fisica_total:outros_creditos_a_vencer_acima_de_5400_dias,
                                        names_to = "modalidade_vencimento",
                                        values_to = "valor"
                                    ) %>%
                                    mutate(
                                        modalidade_split = str_split(
                                            modalidade_vencimento, "_a_vencer|_vencido|_total"
                                        ),
                                        modalidade = purrr::map_chr(modalidade_split, 1),
                                        vencimento = purrr::map_chr(modalidade_split, 2),
                                        vencimento = case_when(
                                            grepl("a_vencer", modalidade_vencimento) ~ paste0("a_vencer", vencimento),
                                            grepl("vencido", modalidade_vencimento) ~ paste0("vencido", vencimento),
                                            grepl("total", modalidade_vencimento) ~ paste0("total", vencimento)
                                        )
                                    )

# transformando em formato longo
carteira_pj_modalidade_prazo <- carteira_pj_modalidade_prazo %>%
                                    select(
                                        instituicao, data, tipo_if, arquivo_origem, codigo,
                                        segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                                        uf, (starts_with("total") | ends_with("total")), everything()
                                    ) %>%
                                    rename(
                                        exterior_pessoa_juridica_total = total_exterior_pessoa_juridica
                                    ) %>%
                                    pivot_longer(
                                        exterior_pessoa_juridica_total:capital_de_giro_rotativo_a_vencer_acima_de_5400_dias, #nolint
                                        names_to = "modalidade_vencimento",
                                        values_to = "valor"
                                    ) %>%
                                    mutate(
                                        modalidade_split = str_split(
                                            modalidade_vencimento, "_a_vencer|_vencido|_total"
                                        ),
                                        modalidade = purrr::map_chr(modalidade_split, 1),
                                        vencimento = purrr::map_chr(modalidade_split, 2),
                                        vencimento = case_when(
                                            grepl("a_vencer", modalidade_vencimento) ~ paste0("a_vencer", vencimento),
                                            grepl("vencido", modalidade_vencimento) ~ paste0("vencido", vencimento),
                                            grepl("total", modalidade_vencimento) ~ paste0("total", vencimento)
                                        )
                                    )

carteira_pf_modalidade_prazo <- carteira_pf_modalidade_prazo %>%
                                    select(-c(modalidade_split, modalidade_vencimento)) %>%
                                    pivot_wider(
                                        names_from = "vencimento",
                                        values_from = "valor"
                                    ) %>%
                                    mutate(
                                        data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                                        across(
                                            c(total_da_carteira_de_pessoa_fisica, total:a_vencer_acima_de_5400_dias),
                                            ~ as.integer(str_replace_all(.x, "\\.", ""))
                                        )
                                    )

carteira_pj_modalidade_prazo <- carteira_pj_modalidade_prazo %>%
                                    select(-c(modalidade_split, modalidade_vencimento)) %>%
                                    pivot_wider(
                                        names_from = "vencimento",
                                        values_from = "valor"
                                    ) %>%
                                    mutate(
                                        data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                                        across(
                                            c(total_da_carteira_de_pessoa_juridica, total:a_vencer_acima_de_5400_dias),
                                            ~ as.integer(str_replace_all(.x, "\\.", ""))
                                        )
                                    )

carteira_pf_modalidade_prazo <- carteira_pf_modalidade_prazo %>%
                                filter(!is.na(data))

carteira_pj_modalidade_prazo <- carteira_pj_modalidade_prazo %>%
                                filter(!is.na(data))

# juntando
carteira_modalidade_prazo <- bind_rows(
                                carteira_pf_modalidade_prazo %>%
                                rename(
                                    total_da_carteira = total_da_carteira_de_pessoa_fisica
                                ) %>%
                                mutate(
                                    tipo_pessoa = "pf"
                                ),
                                carteira_pj_modalidade_prazo %>%
                                rename(
                                    total_da_carteira = total_da_carteira_de_pessoa_juridica
                                ) %>%
                                mutate(
                                    tipo_pessoa = "pj"
                                )
                            )

DBI::dbWriteTable(con_clean, "carteira_modalidade_prazo", carteira_modalidade_prazo, overwrite = TRUE)

rm(carteira_modalidade_prazo, carteira_pf_modalidade_prazo, carteira_pj_modalidade_prazo)