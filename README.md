# Projeto de Engenharia de Dados - Cotação de todas as moedas
## Sumário

[Desafio / Problema](#Desafio-/-Problema)

[Tecnologias Utilizadas](#Tecnologias-Utilizadas)

[Estrutura do Projeto](#Estrutura-do-Projeto)

[Conjunto de Dados](#Conjunto-de-Dados)

[Metodologia](#Metodologia)

[Como Executar](#Como-Executar)

[Resultados](#Resultados)

[Conclusão](#Conclusão)

[Contato](#Contato)


## Desafio / Problema

Em uma situação hipotética, uma empresa brasileira que importa produtos do exterior e os revende no mercado nacional precisa acompanhar diariamente as cotações de moedas estrangeiras divulgadas pelo Banco Central.

Variações no câmbio afetam diretamente os custos de importação e, consequentemente, o preço final repassado aos clientes. No modelo atual, o processo de consulta envolve downloads manuais de arquivos CSV, tratamento de dados em planilhas e cruzamentos manuais — o que gera atrasos e risco de erro.

## Tecnologias Utilizadas

- **Linguagem:** Python e SQL (Postgres).
  
- **Serviços e Ferramentas:** 
  - Apache Airflow (orquestração de dados).
  - Docker (ambiente isolado).
  - Astro CLI (estruturação de projetos Airflow).
  - VS Code (editor de código).
  - pgAdmin 4 (interface para PostgreSQL).

## Estrutura do Projeto

Principais diretórios e arquivos:

```
├── dags/                                    # Diretório com o arquivo DAG.

│   └── quotation.py:                        # Arquivo com a definição do fluxo de trabalho.

├── include/                                 # Diretório para modular o pipeline.

│   ├── staging/                             # Camada de Staging (dados brutos com o mínimo de transformação).

│   │   ├── st_create_table.py               # Criação da tabela de staging.

│   │   ├── st_extract.py                    # Extração dos dados do BCB.

│   │   ├── st_transform.py                  # Transformação dos dados extraídos.

│   │   └── st_load.py                       # Carga dos dados no schema staging.

│   ├── warehouse/                           # Camada de DW (dados tratados e modelados).

│   │   ├── dw_create_table_dim_currency.py  # Criação da dimensão moeda.

│   │   ├── dw_create_table_ft_quotation.py  # Criação da fato cotação.

│   │   ├── dw_extract.py                    # Extração do staging para o DW.

│   │   ├── dw_transform_dim_currency.py     # Transformação da dimensão moeda (SCD2).

│   │   ├── dw_transform_ft_quotation.py     # Transformação da tabela fato.

│   │   ├── dw_load_dim_currency.py          # Carga da dimensão moeda.

│   │   └── dw_load_ft_quotation.py          # Carga da tabela fato.

│   └── utils/                               # Diretório com o arquivo de conexão com o banco de dados.

│       └── db.py                            # Função utilitária para conexão com Postgres.

├── docker-compose.override.yml              # Override do docker-compose padrão para configurar o serviço pgAdmin 4
                                               (interface web para gerenciar bancos PostgreSQL) para rodar no mesmo
                                               container do airflow, definir o volume para persistir dados e conectar
                                               o pgAdmin à rede do Airflow.

└── README.md                                # Documentação do projeto.
```
## Conjunto de Dados

- **Fonte:** Site oficial do Banco Central do Brasil (BCB) - https://www.bcb.gov.br/estabilidadefinanceira/cotacoestodas

- **Endpoint:** https://www4.bcb.gov.br/Download/fechamento/{yyyyMMdd}.csv - Onde {yyyyMMdd} deve ser substituído por uma data no formato ano, mês e dia (ex: 20250623 para 23 de junho de 2025).

- **Retorno:** Arquivo .csv contendo as cotações de todas as moedas disponíveis na data informada, conforme o fechamento oficial da taxa PTAX (referência cambial divulgada pelo Banco Central do Brasil).

- **Formato:** Arquivo CSV disponibilizado via API.

- **Colunas staging:**

>  - **data_fechamento:** Data referente à cotação da moeda (formato AAAA-MM-DD).
>
>  - **moeda_codigo:** Código numérico oficial da moeda (padrão ISO 4217).
>
>  - **moeda_tipo:** Tipo de cotação da moeda estrangeira em relação ao Real e ao Dólar, que influencia o cálculo de conversão.
>
>  - **moeda_descricao:** Sigla da moeda (exemplo: USD, EUR, JPY).
>
>  - **taxa_compra:** Taxa de câmbio utilizada para operações de compra da moeda estrangeira.
>
>  - **taxa_venda:** Taxa de câmbio utilizada para operações de venda da moeda estrangeira.
>
>  - **paridade_compra:** Relação de equivalência entre a moeda estrangeira e o dólar americano (USD) no momento da compra, usada para conversão entre moedas.
>
>  - **paridade_venda:** Relação de equivalência entre a moeda estrangeira e o dólar americano (USD) no momento da venda, usada para conversão entre moedas.
>
>  - **data_processamento:** Data em que os dados foram carregados na área de staging.

- **Colunas dw.ft_cotacao:**

>  - **data_fechamento:** Data referente à cotação da moeda (formato AAAA-MM-DD).
>
>  - **moeda_codigo:** Código numérico oficial da moeda (padrão ISO 4217).
>
>  - **taxa_compra:** Taxa de câmbio utilizada para operações de compra da moeda estrangeira.
>
>  - **taxa_venda:** Taxa de câmbio utilizada para operações de venda da moeda estrangeira.
>
>  - **paridade_compra:** Relação de equivalência entre a moeda estrangeira e o dólar americano (USD) no momento da compra, usada para conversão entre moedas.
>
>  - **paridade_venda:** Relação de equivalência entre a moeda estrangeira e o dólar americano (USD) no momento da venda, usada para conversão entre moedas.
>
>  - **data_processamento:** Data em que os dados foram carregados na área de staging.
 
- **Colunas dw.dim_moeda:**

>  - moeda_id: Chave substituta (surrogate key) usada para controle de versões do registro (Slowly Changing Dimension tipo 2 - SCD2).
>
>  - moeda_codigo: Código numérico oficial da moeda conforme padrão ISO 4217.
>
>  - moeda_tipo: Tipo de cotação da moeda estrangeira em relação ao Real e ao Dólar, que impacta no cálculo das conversões.
>
>  - moeda_descricao: Sigla da moeda (exemplos: USD, EUR, JPY).
>
>  - data_inicio: Data de início da validade do registro na dimensão (quando a versão foi carregada).
>
>  - data_fim: Data de término da validade do registro na dimensão (indica até quando esse registro é válido).
>
>  - registro_ativo: Indicador booleano que identifica se o registro é a versão atual/ativa da moeda (ex: 1 = ativo, 0 = inativo).


## Metodologia

Para agilizar a tomada de decisão dos departamentos Financeiro e Comercial, foi desenvolvida uma pipeline automatizada de ETL baseada na arquitetura Inmon, composta pelas seguintes camadas:

- **Fonte:** Dados públicos de cotações de moedas estrangeiras disponibilizados via API no formato CSV.

- **Staging (Raw / Dados Brutos):** Dados armazenados exatamente como foram extraídos da fonte, com mínima transformação, como padronização de nomes de colunas, formatação de datas e tipos de dados.

- **Data Warehouse (Dados Limpos / Estruturados):** Dados consolidados, integrados e padronizados, seguindo modelagem normalizada (3FN), conforme preconizado pela arquitetura Inmon:

  - **Dimensão:** Tabelas descritivas que armazenam o contexto de negócio. Incluem atributos como descrição da moeda, tipo, vigência (SCD2), etc.
 
  - **Fato**: Tabelas que registram os eventos mensuráveis do negócio. Incluem atributos como taxas de compra/venda, paridade de compra/venda, etc.
 
- **BI:** A partir do Data Warehouse, os dados são disponibilizados para ferramentas de BI (como Power BI, Tableau), possibilitando a criação de dashboards e relatórios interativos.

#### Arquitetura:

![Image](https://github.com/user-attachments/assets/82f4dae2-1d0e-4020-830e-fbafcece35b4)

#### Detalhamento Pipeline:

- **Ingestão de dados (Fonte -> Area Staging):**

  - Inicialização do pipeline às 14:00, conforme agendamento via Airflow.
  
  - Verifica a existência e, se necessário, cria as tabelas staging.cotacoes, dw.ft_cotacao e dw.dim_moeda (isso garante portabilidade entre ambientes, sem a necessidade de pré-configuração no banco de dados).

  - Extração de dados da API do Banco Central, referente à cotação do dia atual, no formato CSV.

  - Transformação leve dos dados: renomeia, padroniza os nomes das colunas e converte tipos de dados (ex: datas, números) e adiciona a data de processamento.

  - Carga na área de Staging. Antes da inserção, remove previamente qualquer dado do mesmo dia, evitando duplicidade no banco.

- **Transformação (Area Staging -> Area DW):**
  - São extraídos apenas os dados referentes à cotação do dia atual, evitando retrabalho com registros históricos.
 
  - Transformação da Tabela de Dimensão (dw.dim_moeda). Inserção da tabela descritiva que armazena o contexto de negócio. Aplica a lógica de Slowly Changing Dimension Tipo 2 (SCD2).
 
  - Transformação da Tabela de Fato (dw.ft_cotacao): Inserção da tabela que registra os eventos mensuráveis do negócio.
 
  - Carga da Dimensão (dw.dim_moeda) Aqui é realizado a lógica do SCD2 para capturar o período em que o registro esteve ativo.
 
  - Carga da Fato (dw.ft_cotacao). Verifica se já existe um registro com a chave composta (data_fechamento + moeda_codigo), se não existir, insere, caso contrário, ignora.

 
- **Modelagem Analítica (Area DW → BI):**
  - Após o carregamento dos dados no Data Warehouse, inicia-se a camada analítica, onde os dados são organizados de forma a atender às necessidades de análise de negócio

#### Pipeline Airflow:

![Image](https://github.com/user-attachments/assets/06936b5c-28e2-45f1-9ce3-9380f283cd17)

#### Performance de cada atividade do Airflow:

![Image](https://github.com/user-attachments/assets/e223709e-ff6a-428d-a817-801982d528f3)

#### Criação de Views para Validação da Carga no Data Warehouse (DW):

1. Volume de registros ativos da tabela dimensão.
   
   Enquanto não houver alterações nos atributos dos registros, o total permanece em 157 registros ativos. A cada modificação nos atributos de algum
   registro da dimensão, esse número é incrementado, refletindo a nova versão do dado conforme a lógica de SCD (Slowly Changing Dimension).

   ![Image](https://github.com/user-attachments/assets/389046bc-7ae2-48e3-bff8-8c897d621bc3)

2. Volume de registros por dia:
   
   Enquanto não houver alterações nos atributos dos registros, o total permanece em 157 registros ativos. A cada modificação nos atributos de algum
   registro da dimensão, esse número é incrementado, refletindo a nova versão do dado conforme a lógica de SCD (Slowly Changing Dimension).
   Como o processo é incremental, o volume total de registros cresce diariamente, refletindo o histórico das alterações realizadas.

   ![Image](https://github.com/user-attachments/assets/7d42506f-9b3c-4818-bb87-6c9cd4cb8d9c)

4. Qualidade da carga realizada. 

   Realiza-se a verificação de valores nulos ou zerados nas colunas, com o objetivo de identificar possíveis falhas ocorridas em qualquer etapa do pipeline de dados.
   Esse controle é essencial para garantir a confiabilidade das informações carregadas, assegurando que os dados estejam adequados para análises e tomada de decisão.

   ![Image](https://github.com/user-attachments/assets/7d5c7530-bbe8-4a7f-9a3f-bec65c9ff941)

## Como Executar

1. Certifique-se de que tenha instalado em sua máquina:
   
   1. **Docker:** Necessário para criar e executar os containers do Airflow.
      
   2. **Astro:** CLI (Astronomer CLI): Utilizado para gerenciar projetos Airflow localmente com Docker de forma simples.
      
   3. **Git:** Para clonar o projeto no GitHub.
  
2. Navegar até o diretório do sistema operacional que deseja baixar o projeto e rodar:
   
```
git clone https://github.com/klebergoes/ED_Pipeline_Airflow_Postgres_Cotacao_Moeda.git
```

3. Iniciar o ambiente Airflow local:
```
astro dev start
```
4. Acesse a interface gráfica do airflow via browser:
```
http://localhost:8080
```
- **Credenciais:**

  - **Login:** admin

  - **Senha:** admin
  
- **Após efetuar login na interface do Airflow, é necessário realizar algumas configurações para o correto funcionamento do pipeline:**
1. Cadastro da Variável com a URL da API:
    1. Acesse a aba “Admin” > “Variables” e clique em “Add Variable”.
    2. Preencha os campos da seguinte forma:
        - Key: BASE_URL
        - Value: https://www4.bcb.gov.br/Download/fechamento/

2. Criação da Conexão com o Banco de Dados Postgres:
    1. Acesse a aba “Admin” > “Connections” e clique em “+ Add a new record”.
    2. Preencha os campos conforme abaixo:
        - Connection ID: postgres_astro
        - Connection Type: postgres
        - Host: airflow_6221ff-postgres-1
        - Login: postgres
        - Password: postgres
        - Port: 5432
        - Schema: Astro

5. Acesse a interface gráfica do pgAdmin 4 via browser:
```
http://localhost:8081
```
- **Credenciais:**

  - **Login:** admin@admin.com

  - **Senha:** admin
 
- **Após realizar login no pgAdmin 4, é necessário criar um novo servidor para acessar o banco de dados:**
1. Criação do Servidor:
    1. Clique com o botão direito em "Servers" e selecione "Create" > "Server..."
  
2. Preenchimento das Informações:
    1. Aba General
        - Name: Postgres_Astro
    2. Aba Connection:
        - Host name / Address: airflow_6221ff-postgres-1
        - Port: 5432
        - Maintenance database: postgres
        - Username: postgres
        - Password: postgres
   
>  **Dica:**
>
>  Para conferir o nome do host (container) e a porta em que o PostgreSQL está rodando, execute:
>  ```
>  docker ps
>  ```
>  Você verá algo como:
>  ```
>  CONTAINER ID   IMAGE                           COMMAND                  CREATED        STATUS         
>  2085ece929b3   postgres:12.6                   "docker-entrypoint.s…"   5 weeks ago    Up 8 minutes   
>
>  PORTS                                              NAMES
>  127.0.0.1:5432->5432/tcp                           airflow_6221ff-postgres-1
>  ```

Agora é só habilitar o pipeline:

![Image](https://github.com/user-attachments/assets/f965fabe-4921-4709-8139-27abe019f0c2)

Aguardar a finalização de execução das atividades:

![Image](https://github.com/user-attachments/assets/d69a241d-42dd-4e62-a42f-be105270c81f)

Conferir no PGadmin 4 se a carga foi realizada com sucesso:

![Image](https://github.com/user-attachments/assets/2fc3c676-1700-4f8d-adb5-2b832b278dcc)

## Resultados

- Redução de Erros Manuais

  - Eliminação de tarefas repetitivas e suscetíveis a falhas humanas.

  - Garantia de consistência e padronização nos processos de ingestão de dados.

- Aumento de Produtividade

  - Profissionais liberados para focar em análises e decisões estratégicas.

  - Processos antes manuais são automatizados, reduzindo o tempo de execução de horas para minutos ou segundos.

- Melhor Monitoramento e Controle

  - Geração automática de logs, alertas e registros de execução.

  - Facilidade para auditoria, rastreabilidade e identificação de gargalos.

- Escalabilidade

  - O mesmo fluxo pode ser reutilizado para múltiplos arquivos, bases, clientes ou regiões com mínimo ajuste.

  - Facilidade para expansão do pipeline com novos dados ou fontes.

- Redução de Custos Operacionais

  - Menor dependência de trabalho manual e retrabalho.

  - Otimização do uso de recursos e tempo da equipe.

- Agilidade na Tomada de Decisão

  - Dados atualizados automaticamente abastecem dashboards e relatórios em tempo real.

  - Redução do tempo entre o dado bruto e a geração de insights.

## Conclusão

Este projeto demonstra minha capacidade de projetar e orquestrar pipelines de dados automatizados e confiáveis utilizando Apache Airflow. A solução entrega valor ao eliminar processos manuais, garantir consistência na movimentação dos dados e possibilitar uma governança mais robusta por meio de logs, agendamentos e dependências bem definidas. Com isso, reforço meus estudos em engenharia de dados aplicada à automação, controle e escalabilidade de fluxos de informação críticos para o negócio.

## Contato

- Autor: Kleber Goes da Silva

- E-mail: kleber-goes@hotmail.com

- LinkedIn: https://www.linkedin.com/in/kleber-goes-02091990/
