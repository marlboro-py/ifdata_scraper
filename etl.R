#npl90+
#ROE ou ROI
#INDICE DE EFICIENCIA
#MARGEM FINANCEIRA BRUTA E SPREAD
#INDICE DE BASILEIA 
#CAPITAL 1 E CAPITAL 2
#TAMANHO DA CARTEIRA DE CREDITO
#DIVERSIFICACAO DA CARTEIRA DE CREDITO
#CUSTO DE CREDITO (PROVISAO / CARTEIRA)
#NIVEL DE PAYOUT (% DE LL UTILIZADO PRA PAGAMENTO DE DIVIDENDOS)
#P&L
#NIVEL DE PATRIMONIO LIQUIDO
#COMPOSICAO 4966 (YoY)
#CUSTO DE CAPTACAO MEDIO
#DEPOSITOS - DISCRIMINADOS

library(tidyverse)
library(here)

con <- DBI::dbConnect(duckdb::duckdb(), here("repos/ifdata_scraper/ifdata_raw.duckdb"))
con_clean <- DBI::dbConnect(duckdb::duckdb(), here("repos/ifdata_scraper/ifdata_clean.duckdb"))

###### ATIVO ######
ativo <- con %>%
            tbl("ativo") %>%
            collect() %>%
            janitor::clean_names()

# transformando colunas
ativo <- ativo %>%
            mutate(
                instituicao = coalesce(instituicao, ativos_instituicoes, instituicao_financeira),
                tc = coalesce(tc, ativos_tc),
                td = coalesce(td, ativos_td),
                data = coalesce(data, ativos_data_balancete)
            ) %>%
            select(-c(
                ativos_instituicoes, instituicao_financeira, ativos_tc,
                ativos_td, ativos_data_balancete, ativos_ranking, ativos_obs
            )) %>%
            select(
                instituicao, data, tipo_if, arquivo_origem, codigo, starts_with("conglomerado"),
                tcb, tc, ti, td, sr, cidade, uf, everything()
            ) %>%
            mutate(
                data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                across(
                    disponibilidades_a:ativos_permanente, ~ as.integer(str_replace_all(.x, "\\.", ""))
                )
            )

# datas NAs sao consolidados dos arquivos - da pra filtrar fora
ativo <- ativo %>%
            filter(!is.na(data))

# retrofit de colunas - pegar so colunas em comum com todos os reports antigos
ativo <- ativo %>%
            mutate(
                tvm_e_instrumentos_financeiros_derivativos_2025 = (
                    titulos_e_valores_mobiliarios_c + instrumentos_derivativos_d
                ),
                oper_cred_e_arrend_mercantil_2024 = (
                    operacoes_de_credito_operacoes_de_credito_d1 +
                    arrendamento_mercantil_arrendamento_mercantil_a_receber_e1 +
                    arrendamento_mercantil_imobilizado_de_arrendamento_e2
                ),
                oper_cred_e_arrend_mercantil_2025 = (
                    operacoes_de_credito_valor_contabil_bruto_e1 +
                    operacoes_de_arrendamento_financeiro_valor_contabil_bruto_f1
                )
            ) %>%
            mutate(
                disponibilidades_a = coalesce(disponibilidades_a, ativos_disponibilidades),
                aplicacoes_interfinanceiras_de_liquidez_b = coalesce(
                    aplicacoes_interfinanceiras_de_liquidez_b, ativos_aplicacoes_interfinanceiras
                ),
                tvm_e_instrumentos_financeiros_derivativos_c = coalesce(
                    tvm_e_instrumentos_financeiros_derivativos_c,
                    ativos_tvm_e_instrumentos_financeiros_derivativos,
                    tvm_e_instrumentos_financeiros_derivativos_2025
                ),
                oper_cred_e_arrend_mercantil = coalesce(
                    ativos_oper_cred_e_arrend_mercantil_total, oper_cred_e_arrend_mercantil_2024,
                    oper_cred_e_arrend_mercantil_2025
                ),
                ativo_total_k_a_b_c_d_e_f_g_h_i_j = coalesce(
                    ativo_total_k_a_b_c_d_e_f_g_h_i_j, ativo_total_k_i_j, ativos_ativo_total
                ),
                ativo_permanente_j = coalesce(
                    ativo_permanente_j, permanente_ajustado_h, ativos_permanente
                ),
                outros_ativos_realizaveis_i = coalesce(
                    outros_ativos_realizaveis_i, outros_ativos_realizaveis_g, ativos_outros_valores_e_bens
                )
            ) %>%
            rename(
                disponibilidades = disponibilidades_a,
                aplicacoes_interfinanceiras_liquidez = aplicacoes_interfinanceiras_de_liquidez_b,
                tvm_e_derivativos = tvm_e_instrumentos_financeiros_derivativos_c,
                outros_ativos_realizaveis = outros_ativos_realizaveis_i,
                ativo_permanente = ativo_permanente_j,
                ativo_total = ativo_total_k_a_b_c_d_e_f_g_h_i_j
            ) %>%
            select(
                instituicao, data, tipo_if, arquivo_origem, codigo, starts_with("conglomerado"),
                tcb, tc, ti, td, sr, cidade, uf, disponibilidades, aplicacoes_interfinanceiras_liquidez,
                tvm_e_derivativos, oper_cred_e_arrend_mercantil, ativo_total, ativo_permanente,
                outros_ativos_realizaveis
            )

DBI::dbWriteTable(con_clean, "ativo", ativo, overwrite = TRUE)
rm(ativo)

###### CARTEIRAS DE CREDITO - MODALIDADE E PRAZO #######
carteira_pf_modalidade_prazo <- con %>%
                                    tbl("carteira_de_credito_ativa_pessoa_fisica_modalidade_e_prazo_de_vencimento") %>%
                                    collect()

carteira_pj_modalidade_prazo <- con %>%
                                    tbl("carteira_de_credito_ativa_pessoa_juridica_modalidade_e_prazo_de_vencimento") %>% #nolint
                                    collect()

# transformando em formato longo
carteira_pf_modalidade_prazo <- carteira_pf_modalidade_prazo %>%
                                    mutate(
                                        instituicao = coalesce(instituicao, instituicao_financeira)
                                    ) %>%
                                    select(-instituicao_financeira) %>%
                                    select(
                                        instituicao, data, tipo_if, arquivo_origem, codigo,
                                        segmento, tcb, td, tc, sr, cidade,
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
                                    mutate(
                                        instituicao = coalesce(instituicao, instituicao_financeira)
                                    ) %>%
                                    select(-instituicao_financeira) %>%
                                    select(
                                        instituicao, data, tipo_if, arquivo_origem, codigo,
                                        segmento, tcb, td, tc, sr, cidade,
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

###### CARTEIRA DE CREDITO - OUTROS #######
carteira_pj_atv_econ <- con %>%
                         tbl("carteira_de_credito_ativa_pessoa_juridica_por_atividade_economica") %>%
                         collect()

carteira_pj_atv_econ <- carteira_pj_atv_econ %>%
                            select(
                                instituicao, data, tipo_if, arquivo_origem, codigo,
                                segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                                uf, (starts_with("total") | ends_with("total")), everything()
                            ) %>%
                            rename(
                                nao_individualizado_pessoa_juridica_total = total_nao_individualizado_pessoa_juridica,
                                exterior_pessoa_juridica_total = total_exterior_pessoa_juridica
                            ) %>%
                            pivot_longer(
                                nao_individualizado_pessoa_juridica_total:outros_a_vencer_acima_de_5400_dias,
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

carteira_pj_atv_econ <- carteira_pj_atv_econ %>%
                                    select(-c(modalidade_split, modalidade_vencimento)) %>%
                                    pivot_wider(
                                        names_from = "vencimento",
                                        values_from = "valor"
                                    ) %>%
                                    select(
                                        instituicao, data, tipo_if, arquivo_origem, codigo,
                                        segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                                        uf, modalidade, everything()
                                    ) %>%
                                    mutate(
                                        data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                                        across(
                                            (
                                                starts_with("total") | starts_with("vencido") | starts_with("a_vencer")
                                                | starts_with("atividade")
                                            ),
                                            ~ as.integer(str_replace_all(.x, "\\.", ""))
                                        )
                                    )

carteira_pj_atv_econ <- carteira_pj_atv_econ %>%
                            filter(!is.na(data))

DBI::dbWriteTable(con_clean, "carteira_pj_atv_econ", carteira_pj_atv_econ, overwrite = TRUE)
rm(carteira_pj_atv_econ)

carteira_pj_porte <- con %>%
                        tbl("carteira_de_credito_ativa_pessoa_juridica_por_porte_do_tomador") %>%
                        collect()

carteira_pj_porte <- carteira_pj_porte %>%
                        select(
                            instituicao, data, tipo_if, arquivo_origem, codigo,
                            segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                            uf, (starts_with("total") | ends_with("total")), everything()
                        ) %>%
                        rename(
                            nao_individualizado_pessoa_juridica_total = total_nao_individualizado_pessoa_juridica,
                            exterior_pessoa_juridica_total = total_exterior_pessoa_juridica
                        ) %>%
                        pivot_longer(
                            micro:indisponivel,
                            names_to = "porte",
                            values_to = "valor"
                        ) %>%
                        select(
                            instituicao, data, tipo_if, arquivo_origem, codigo,
                            segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                            uf, everything()
                        ) %>%
                        mutate(
                            data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                            across(
                                total_da_carteira_de_pessoa_juridica:valor,
                                ~ as.integer(str_replace_all(.x, "\\.", ""))
                            )
                        )

carteira_pj_porte <- carteira_pj_porte %>%
                        filter(!is.na(data))

DBI::dbWriteTable(con_clean, "carteira_pj_porte", carteira_pj_porte, overwrite = TRUE)

rm(carteira_pj_porte)

carteira_instr_fin <- con %>%
                            tbl("carteira_de_credito_ativa_por_carteiras_de_instrumentos_financeiros") %>%
                            collect()

carteira_instr_fin <- carteira_instr_fin %>%
                            select(
                                instituicao, data, tipo_if, arquivo_origem, codigo,
                                tcb, td, tc, sr, cidade, uf, (starts_with("total")),
                                everything()
                            ) %>%
                            pivot_longer(
                                carteiras_c1:carteira_nao_informada_ou_nao_se_aplica,
                                names_to = "tipo_carteira",
                                values_to = "valor"
                            ) %>%
                            select(
                                instituicao, data, tipo_if, arquivo_origem, codigo,
                                tcb, td, tc, sr, cidade, uf, tipo_carteira, (starts_with("total")),
                                everything()
                            ) %>%
                            mutate(
                                data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                                tipo_carteira = str_replace_all(tipo_carteira, "carteira_|carteiras_", ""),
                                across(total_geral:valor, ~ as.integer(str_replace_all(.x, "\\.", "")))
                            )

carteira_instr_fin <- carteira_instr_fin %>%
                        filter(!is.na(data))

DBI::dbWriteTable(con_clean, "carteira_instr_fin", carteira_instr_fin, overwrite = TRUE)
rm(carteira_instr_fin)

carteira_indexador <- con %>%
                        tbl("carteira_de_credito_ativa_por_indexador") %>%
                        collect()

carteira_indexador <- carteira_indexador %>%
                        janitor::clean_names() %>%
                        select(
                            instituicao, data, tipo_if, arquivo_origem, codigo,
                            segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                            uf, (starts_with("total")), everything()
                        ) %>%
                        rename(
                            total_prefixado = prefixado
                        ) %>%
                        pivot_longer(
                            tr_tbf:trfc_pos,
                            names_to = "indice",
                            values_to = "valor"
                        ) %>%
                        select(
                            instituicao, data, tipo_if, arquivo_origem, codigo,
                            segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                            uf, indice, (starts_with("total")), everything()
                        ) %>%
                        mutate(
                            data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                            tipo_indice = if_else(grepl("pre", indice), "prefixado", "posfixado"),
                            across(total_geral:valor, ~ as.integer(str_replace_all(.x, "\\.", "")))
                        )

carteira_indexador <- carteira_indexador %>%
                        filter(!is.na(data))

DBI::dbWriteTable(con_clean, "carteira_indexador", carteira_indexador, overwrite = TRUE)
rm(carteira_indexador)

carteira_nivel_risco <- con %>%
                            tbl("carteira_de_credito_ativa_por_nivel_de_risco_da_operacao") %>%
                            collect()


carteira_nivel_risco <- carteira_nivel_risco %>%
                        select(
                            instituicao, data, tipo_if, arquivo_origem, codigo,
                            segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                            uf, (starts_with("total")), everything()
                        ) %>%
                        pivot_longer(
                            aa:h,
                            names_to = "nivel_risco",
                            values_to = "valor"
                        ) %>%
                        select(
                            instituicao, data, tipo_if, arquivo_origem, codigo,
                            segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                            uf, nivel_risco, (starts_with("total")), everything()
                        ) %>%
                        mutate(
                            data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                            across(total_geral:valor, ~ as.integer(str_replace_all(.x, "\\.", "")))
                        )

carteira_nivel_risco <- carteira_nivel_risco %>%
                            filter(!is.na(data))

DBI::dbWriteTable(con_clean, "carteira_nivel_risco", carteira_nivel_risco, overwrite = TRUE)
rm(carteira_nivel_risco)

carteira_regiao <- con %>%
                    tbl("carteira_de_credito_ativa_por_regiao_geografica") %>%
                    collect()

carteira_regiao <- carteira_regiao %>%
                    select(
                        instituicao, data, tipo_if, arquivo_origem, codigo,
                        segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                        uf, (starts_with("total")), everything()
                    ) %>%
                    pivot_longer(
                        sudeste:regiao_nao_informada,
                        names_to = "regiao",
                        values_to = "valor"
                    ) %>%
                    select(
                        instituicao, data, tipo_if, arquivo_origem, codigo,
                        segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                        uf, regiao, (starts_with("total")), everything()
                    )  %>%
                    mutate(
                        data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                        across(total_geral:valor, ~ as.integer(str_replace_all(.x, "\\.", "")))
                    )

carteira_regiao <- carteira_regiao %>%
                    filter(!is.na(data))

DBI::dbWriteTable(con_clean, "carteira_regiao", carteira_regiao, overwrite = TRUE)
rm(carteira_regiao)

carteira_qtd_cli_operacoes <- con %>%
                                tbl("carteira_de_credito_ativa_quantidade_de_clientes_e_de_operacoes") %>%
                                collect()

carteira_qtd_cli_operacoes <- carteira_qtd_cli_operacoes %>%
                                select(
                                    instituicao, data, tipo_if, arquivo_origem, codigo,
                                    segmento, instituicao_financeira, tcb, td, tc, sr, cidade,
                                    uf, starts_with("quantidade")
                                ) %>%
                                mutate(
                                    data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                                    across(starts_with("quantidade"), ~ as.integer(str_replace_all(.x, "\\.", "")))
                                )

carteira_qtd_cli_operacoes <- carteira_qtd_cli_operacoes %>%
                                filter(!is.na(data))

DBI::dbWriteTable(con_clean, "carteira_qtd_cli_operacoes", carteira_qtd_cli_operacoes, overwrite = TRUE)
rm(carteira_qtd_cli_operacoes)

###### DEMONSTRACAO DE RESULTADO ######
# informacoes do legado estao divididas em resultado liquido e resultado da intermediacao financeira
# comecar pegando as infos que preciso pra demonstracao de resultado atual

resultado_liquido <- con %>%
                        tbl("resultado_liquido") %>%
                        collect() %>%
                        janitor::clean_names()

resultado_liquido <- resultado_liquido %>%
                        rename_with(~ str_replace(.x, "resultado_liquido_", "")) %>%
                        rename_with(~ str_replace(.x, "outras_receitasdespesas_operacionais_", "")) %>%
                        rename(
                            instituicao = instituicoes,
                            data = data_balancete,
                            receitas_servicos = receitas_de_prestacao_de_servicos,
                            despesas_pessoal = despesas_de_pessoal,
                            despesas_adm = outras_despesas_administ,
                            result_part_colig_cont = result_de_part_em_coligadas_e_controladas,
                            lair = resultado_antes_da_tributacao_lucro_e_part,
                            ir_csll = ir_e_contrib_social,
                            particip_lucros = particip_nos_lucros
                        ) %>%
                        select(
                            ranking, instituicao, td, tc, obs, data, tipo_if, arquivo_origem,
                            everything()
                        ) %>%
                        mutate(
                            instituicao = trimws(instituicao),
                            obs = trimws(obs),
                            data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                            across(receitas_servicos:no_de_agencias, ~ as.integer(str_replace_all(.x, "\\.", "")))
                        )

resultado_liquido <- resultado_liquido %>%
                        filter(!is.na(data))

resultado_intermed_fin <- con %>%
                            tbl("resultado_da_intermediacao_financeira") %>%
                            collect() %>%
                            janitor::clean_names()

resultado_intermed_fin <- resultado_intermed_fin %>%
                            rename_with(~ str_replace(.x, "resultado_da_intermediacao_financeira_", "")) %>%
                            rename_with(~ str_replace(.x, "receitas_de_intermediacao_financeira_", "receitas_intermed_")) %>% # nolint
                            rename_with(~ str_replace(.x, "despesas_de_intermediacao_financeira_", "despesas_intermed_")) %>% # nolint
                            rename(
                                instituicao = instituicoes,
                                data = data_balancete
                            ) %>%
                            select(
                                ranking, instituicao, td, tc, obs, data, tipo_if, arquivo_origem,
                                everything()
                            ) %>%
                            mutate(
                                instituicao = trimws(instituicao),
                                obs = trimws(obs),
                                data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                                across(
                                    receitas_intermed_operacoes_de_cred_e_arrend_mercantil:resultado_bruto,
                                     ~ as.integer(str_replace_all(.x, "\\.", ""))
                                )
                            )

resultado_intermed_fin <- resultado_intermed_fin %>%
                            filter(!is.na(data))

dre_legado <- resultado_intermed_fin %>%
                left_join(
                    resultado_liquido %>%
                    rename(arquivo_origem_result_liquido = arquivo_origem) %>%
                    select(-c(ranking, td, tc, obs, tipo_if)),
                    by = c("instituicao", "data")
                ) %>%
                mutate(
                    arquivo_origem = paste0(arquivo_origem, "|", arquivo_origem_result_liquido)
                ) %>%
                select(-arquivo_origem_result_liquido)

rm(resultado_intermed_fin, resultado_liquido)

dre_legado <- dre_legado %>%
                mutate(
                    receitas_intermed = rowSums(across(starts_with("receitas_intermed"))),
                    despesas_intermed = rowSums(across(starts_with("despesas_intermed")))
                )

demonstracao_resultado <- con %>%
                            tbl("demonstracao_de_resultado") %>%
                            collect() %>%
                            janitor::clean_names()

demonstracao_resultado <- demonstracao_resultado %>%
                            rename_with(~ str_replace(.x, "resultado_de_intermediacao_financeira_receitas_de_intermediacao_financeira_", "receitas_intermed_")) %>% # nolint
                            rename_with(~ str_replace(.x, "resultado_de_intermediacao_financeira_despesas_de_intermediacao_financeira_", "despesas_intermed_")) %>% #nolint
                            rename_with(~ str_replace(.x, "outras_receitasdespesas_operacionais_", "")) %>%
                            rename_with(~ str_replace(.x, "resultado_de_intermediacao_financeira_rendas_de_aplicacoes_interfinanceiras_de_liquidez_", "receitas_interf_")) %>% #nolint
                            rename_with(~ str_replace(.x, "resultado_de_intermediacao_financeira_rendas_de_titulos_e_valores_mobiliarios_", "receitas_tvm_")) %>% #nolint 
                            rename_with(~ str_replace(.x, "resultado_de_intermediacao_financeira_rendas_de_operacoes_de_credito_", "receitas_cred_")) %>% #nolint
                            rename_with(~ str_replace(.x, "resultado_de_intermediacao_financeira_resultado_com_perda_esperada_resultado_com_perda_esperada_", "resultados_pe_")) %>% #nolint
                            rename_with(~ str_replace(.x, "resultado_de_intermediacao_financeira_despesas_de_captacoes_", "despesas_captacoes_")) %>% # nolint
                            select(
                                instituicao, instituicao_financeira, data, tipo_if, arquivo_origem, codigo,
                                starts_with("conglomerado"), tcb, tc, ti, td, sr, cidade, uf, everything()
                            ) %>%
                            mutate(
                                data = as.Date(paste0("01/", data), format = "%d/%m/%Y"),
                                across(
                                    receitas_intermed_rendas_de_operacoes_de_credito_a1:lucro_liquido_aa_x_y_z,
                                    ~ as.integer(str_replace_all(.x, "\\.", ""))
                                )
                            )

# mudanca regulatoria em 2025
# nao existe equivalente direto entre as despesas
# TODO: REVISAR RETROFIT DE RECEITAS E DESPESAS
demonstracao_resultado <- demonstracao_resultado %>%
                            mutate(
                                instituicao = coalesce(instituicao, instituicao_financeira),
                                resultado_cambio_2025 = (
                                    receitas_interf_ajuste_de_variacao_cambial_de_aplicacoes_interfinanceiras_de_liquidez_a2 + #nolint
                                    receitas_tvm_ajuste_de_variacao_cambial_de_tvm_b2 +
                                    receitas_cred_ajuste_de_variacao_cambial_de_operacoes_de_credito_c2
                                ),
                                receitas_intermed_2025 = (
                                    receitas_interf_rendas_de_aplicacoes_interfinanceiras_de_liquidez_a +
                                    receitas_tvm_rendas_de_titulos_e_valores_mobiliarios_b +
                                    receitas_cred_rendas_de_operacoes_de_credito_c +
                                    resultado_de_intermediacao_financeira_rendas_de_arrendamento_financeiro_d +
                                    resultado_de_intermediacao_financeira_rendas_de_outras_operacoes_com_caracteristicas_de_concessao_de_credito_e + #nolint
                                    resultado_de_intermediacao_financeira_resultado_com_derivativos_i +
                                    resultado_de_intermediacao_financeira_outros_resultados_de_intermediacao_financeira_j #nolint
                                ),
                                despesas_intermed_2025 = (
                                    despesas_captacoes_despesas_de_captacoes_g +
                                    resultado_de_intermediacao_financeira_despesas_de_instrumentos_de_divida_elegiveis_a_capital_h + #nolint
                                    abs(resultados_pe_f)
                                ),
                                resultado_operacional_2025 = (
                                    resultado_de_intermediacao_financeira_resultado_de_intermediacao_financeira_k_a_b_c_d_e_f_g_h_i_j + #nolint
                                    outras_receitas_despesas_outras_receitas_despesas_w_m_n_o_p_q_r_s_t_u_v
                                ),
                                resultado_nao_operacional_2025 = (
                                    resultado_antes_da_tributacao_e_participacoes_x_k_l_w - resultado_operacional_2025
                                )
                            ) %>%
                            mutate(
                                receitas_intermed_rendas_de_operacoes_de_credito_a1 = coalesce(
                                    receitas_intermed_rendas_de_operacoes_de_credito_a1,
                                    receitas_cred_rendas_de_operacoes_de_credito_c
                                ),
                                receitas_intermed_rendas_de_operacoes_de_arrendamento_mercantil_a2 = coalesce(
                                    receitas_intermed_rendas_de_operacoes_de_arrendamento_mercantil_a2,
                                    resultado_de_intermediacao_financeira_rendas_de_arrendamento_financeiro_d
                                ),
                                receitas_intermed_rendas_de_operacoes_com_tvm_a3 = coalesce(
                                    receitas_intermed_rendas_de_operacoes_com_tvm_a3,
                                    receitas_tvm_rendas_de_titulos_e_valores_mobiliarios_b
                                ),
                                receitas_intermed_rendas_de_operacoes_com_instrumentos_financeiros_derivativos_a4 = coalesce( #nolint
                                    receitas_intermed_rendas_de_operacoes_com_instrumentos_financeiros_derivativos_a4,
                                    resultado_de_intermediacao_financeira_resultado_com_derivativos_i
                                ),
                                receitas_intermed_resultado_de_operacoes_de_cambio_a5 = coalesce(
                                    receitas_intermed_resultado_de_operacoes_de_cambio_a5,
                                    resultado_cambio_2025
                                ),
                                receitas_intermed_rendas_de_aplicacoes_compulsorias_a6 = coalesce(
                                    receitas_intermed_rendas_de_aplicacoes_compulsorias_a6,
                                    receitas_interf_rendas_de_aplicacoes_interfinanceiras_de_liquidez_a
                                ),
                                receitas_intermed_receitas_de_intermediacao_financeira_a_a1_a2_a3_a4_a5_a6 = coalesce(
                                    receitas_intermed_receitas_de_intermediacao_financeira_a_a1_a2_a3_a4_a5_a6,
                                    receitas_intermed_2025
                                ),
                                despesas_intermed_despesas_de_captacao_b1 = coalesce(
                                    despesas_intermed_despesas_de_captacao_b1,
                                    despesas_captacoes_despesas_de_captacoes_g
                                ),
                                despesas_intermed_despesas_de_intermediacao_financeira_b_b1_b2_b3_b4_b5 = coalesce(
                                    despesas_intermed_despesas_de_intermediacao_financeira_b_b1_b2_b3_b4_b5,
                                    despesas_intermed_2025
                                ),
                                resultado_de_intermediacao_financeira_resultado_de_intermediacao_financeira_c_a_b = coalesce( #nolint
                                    resultado_de_intermediacao_financeira_resultado_de_intermediacao_financeira_c_a_b,
                                    resultado_de_intermediacao_financeira_resultado_de_intermediacao_financeira_k_a_b_c_d_e_f_g_h_i_j #nolint
                                ),
                                rendas_de_prestacao_de_servicos_d1 = coalesce(
                                    rendas_de_prestacao_de_servicos_d1,
                                    outras_receitas_despesas_outras_rendas_de_prestacao_de_servicos_n
                                ),
                                rendas_de_tarifas_bancarias_d2 = coalesce(
                                    rendas_de_tarifas_bancarias_d2,
                                    outras_receitas_despesas_rendas_de_tarifas_bancarias_m
                                ),
                                despesas_de_pessoal_d3 = coalesce(
                                    despesas_de_pessoal_d3,
                                    outras_receitas_despesas_despesas_de_pessoal_o
                                ),
                                despesas_administrativas_d4 = coalesce(
                                    despesas_administrativas_d4,
                                    outras_receitas_despesas_despesas_administrativas_p
                                ),
                                despesas_tributarias_d5 = coalesce(
                                    despesas_tributarias_d5,
                                    outras_receitas_despesas_despesas_tributarias_s
                                ),
                                resultado_de_participacoes_d6 = coalesce(
                                    resultado_de_participacoes_d6,
                                    outras_receitas_despesas_resultado_de_participacoes_t
                                ),
                                outras_receitas_operacionais_d7 = coalesce(
                                    outras_receitas_operacionais_d7,
                                    outras_receitas_despesas_outras_receitas_u
                                ),
                                outras_despesas_operacionais_d8 = coalesce(
                                    outras_despesas_operacionais_d8,
                                    outras_receitas_despesas_outras_despesas_v
                                ),
                                outras_receitasdespesas_operacionais_d_d1_d2_d3_d4_d5_d6_d7_d8 = coalesce(
                                    outras_receitasdespesas_operacionais_d_d1_d2_d3_d4_d5_d6_d7_d8,
                                    outras_receitas_despesas_outras_receitas_despesas_w_m_n_o_p_q_r_s_t_u_v
                                ),
                                resultado_operacional_e_c_d = coalesce(
                                    resultado_operacional_e_c_d,
                                    resultado_operacional_2025
                                ),
                                resultado_nao_operacional_f = coalesce(
                                   resultado_nao_operacional_f,
                                   resultado_nao_operacional_2025
                                ),
                                resultado_antes_da_tributacao_lucro_e_participacao_g_e_f = coalesce(
                                    resultado_antes_da_tributacao_lucro_e_participacao_g_e_f,
                                    resultado_antes_da_tributacao_e_participacoes_x_k_l_w
                                ),
                                imposto_de_renda_e_contribuicao_social_h = coalesce(
                                    imposto_de_renda_e_contribuicao_social_h,
                                    imposto_de_renda_e_contribuicao_social_y
                                ),
                                participacao_nos_lucros_i = coalesce(
                                    participacao_nos_lucros_i,
                                    participacoes_no_lucro_z
                                ),
                                lucro_liquido_j_g_h_i = coalesce(
                                    lucro_liquido_j_g_h_i,
                                    lucro_liquido_aa_x_y_z
                                )
                            ) %>%
                            mutate(
                                receitas_intermed_operacoes_de_cred_e_arrend_mercantil = (
                                    receitas_intermed_rendas_de_operacoes_de_credito_a1 +
                                    receitas_intermed_rendas_de_operacoes_de_arrendamento_mercantil_a2
                                )
                            ) %>%
                            rename(
                                tmp_receitas_intermed_operacoes_com_tit_val_mobiliarios = receitas_intermed_rendas_de_operacoes_com_tvm_a3, # nolint
                                tmp_receitas_intermed_operacoes_com_instrum_financ_derivativos = receitas_intermed_rendas_de_operacoes_com_instrumentos_financeiros_derivativos_a4, #nolint
                                tmp_receitas_intermed_operacoes_de_cambio = receitas_intermed_resultado_de_operacoes_de_cambio_a5, #nolint
                                tmp_receitas_intermed_aplicacoes_compulsorias = receitas_intermed_rendas_de_aplicacoes_compulsorias_a6, #nolint
                                tmp_despesas_intermed_captacoes_no_mercado = despesas_intermed_despesas_de_captacao_b1,
                                tmp_despesas_intermed_emprestimos_e_repasses = despesas_intermed_despesas_de_obrigacoes_por_emprestimos_e_repasses_b2, # nolint
                                tmp_despesas_intermed_arrendam_mercantil = despesas_intermed_despesas_de_operacoes_de_arrendamento_mercantil_b3, #nolint
                                tmp_despesas_intermed_operacoes_de_cambio = despesas_intermed_resultado_de_operacoes_de_cambio_b4, # nolint
                                tmp_despesas_intermed_provisao_para_cl = despesas_intermed_resultado_de_provisao_para_creditos_de_dificil_liquidacao_b5, # nolint
                                tmp_receitas_intermed = receitas_intermed_receitas_de_intermediacao_financeira_a_a1_a2_a3_a4_a5_a6, # nolint
                                tmp_despesas_intermed = despesas_intermed_despesas_de_intermediacao_financeira_b_b1_b2_b3_b4_b5, # nolint
                                tmp_receitas_servicos = rendas_de_prestacao_de_servicos_d1,
                                tmp_receitas_tarifa = rendas_de_tarifas_bancarias_d2,
                                tmp_despesas_pessoal = despesas_de_pessoal_d3,
                                tmp_despesas_adm = despesas_administrativas_d4,
                                tmp_despesas_tributarias = despesas_tributarias_d5,
                                tmp_outras_despesas_operacionais = outras_despesas_operacionais_d8,
                                tmp_result_part_colig_cont = resultado_de_participacoes_d6,
                                tmp_outras_receitas_operacionais = outras_receitas_operacionais_d7,
                                tmp_resultado_operacional = resultado_operacional_e_c_d,
                                tmp_resultado_nao_operacional = resultado_nao_operacional_f,
                                tmp_lair = resultado_antes_da_tributacao_lucro_e_participacao_g_e_f,
                                tmp_ir_csll = imposto_de_renda_e_contribuicao_social_h,
                                tmp_particip_lucros = participacao_nos_lucros_i,
                                tmp_lucro_liquido = lucro_liquido_j_g_h_i
                            ) %>%
                            select(
                                instituicao, data, tipo_if, arquivo_origem, codigo,
                                starts_with("conglomerado"), tcb, tc, ti, td, sr, cidade, uf, starts_with("tmp")
                            ) %>%
                            rename_with(
                                ~ str_replace(.x, "tmp_", "")
                            )

demonstracao_resultado <- demonstracao_resultado %>%
                            filter(!is.na(data))

cols_faltantes_legado <- colnames(demonstracao_resultado)
cols_faltantes_legado <- cols_faltantes_legado[!(cols_faltantes_legado %in% colnames(dre_legado))]

dre_legado[cols_faltantes_legado] <- NA

demonstracao_resultado <- demonstracao_resultado %>%
                            rbind(
                                dre_legado %>%
                                select(all_of(colnames(demonstracao_resultado)))
                            ) %>%
                            mutate(
                                resultado_bruto = receitas_intermed - despesas_intermed
                            )

DBI::dbWriteTable(con_clean, "demonstracao_resultado", demonstracao_resultado, overwrite = TRUE)
rm(demonstracao_resultado, dre_legado, cols_faltantes_legado)

###### DEPOSITOS ######: nao entra pq eh so legado e eh no passivo

###### INFORMACOES DE CAPITAL ######
informacoes_capital <- con %>%
                        tbl("informacoes_de_capital") %>%
                        collect() %>%
                        janitor::clean_names()

movimentacao_cambio <- con %>%
                        tbl("movimentacao_de_cambio_no_trimestre") %>%
                        collect()

passivo <- con %>%
            tbl("passivo") %>%
            collect()

resumo <- con %>%
            tbl("resumo") %>%
            collect()

segmentacao <- con %>%
                tbl("segmentacao") %>%
                collect()