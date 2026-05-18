# prod_dab-bundle-analytics

this is for lakehouse analytics

# Store Project 🏪

Um projeto de **Engenharia de Dados** construído com **Databricks Asset Bundles (DAB)** para automatizar o processamento e ingestão de dados no catálogo Northwind. Este projeto implementa uma arquitetura moderna de infraestrutura como código (IaC) para gerenciar jobs, pipelines e recursos de dados em ambientes de desenvolvimento e produção.

---

## 📋 Visão Geral do Projeto

O **Store Project** é uma solução de ponta a ponta para orquestração e processamento de dados utilizando a plataforma Databricks. O projeto segue as melhores práticas de engenharia de software, incluindo:

- **Infraestrutura como Código (IaC)**: Definição de recursos via YAML (Databricks Asset Bundles)
- **Ambientes Múltiplos**: Suporte para desenvolvimento (dev) e produção (prod) com configurações isoladas
- **Automação**: Jobs agendados para execução periódica de pipelines de dados
- **Catálogo Centralizado**: Uso do catálogo Northwind para armazenamento de dados
- **Testes Automatizados**: Cobertura de testes unitários para código compartilhado

---

## 🎯 Objetivos Principais

1. **Ingestão Automatizada**: Executa notebooks de ingestão de dados através do job `demo2025`
2. **Escalabilidade**: Infraestrutura pronta para crescimento com ambientes isolados
3. **Rastreabilidade**: Controle de versão de todas as configurações e código
4. **Manutenibilidade**: Código bem estruturado e documentado para facilitar manutenção
5. **Segurança**: Permissões granulares e isolamento de ambientes

---

## 📁 Estrutura do Projeto

```
store_project/
├── .databricks/                 # Configurações locais do Databricks
├── .vscode/                     # Configurações do VS Code
├── databricks.yml               # Definição principal do Asset Bundle
├── pyproject.toml              # Configuração do projeto Python e dependências
├── README.md                    # Este arquivo
│
├── src/                         # Código-fonte Python
│   └── pipeline/
│       └── noteboks.ipynb       # Notebook de ingestão de dados
│
├── resources/                   # Definições de recursos Databricks
│   └── jobs/
│       └── demo.yml             # Definição do job "demo2025"
│
├── tests/                       # Testes unitários
│   └── (testes do código compartilhado)
│
└── fixtures/                    # Dados de teste e fixtures
    └── (conjuntos de dados para testes)
```

### Descrição dos Diretórios

| Diretório | Descrição |
|-----------|-----------|
| **src/** | Código Python reutilizável para jobs, pipelines e transformações |
| **resources/** | Configurações YAML dos recursos Databricks (jobs, pipelines, workflows) |
| **tests/** | Testes unitários executados com pytest para validar a lógica de negócio |
| **fixtures/** | Dados de exemplo e conjuntos de dados para testes |
| **.databricks/** | Metadados e configurações específicas do ambiente Databricks |

---


---

## 🔄 Pipeline

### Bronze Layer
- Ingestão de dados brutos
- Fonte: arquivos ou sistemas externos

### Silver Layer
- Limpeza e padronização
- Tratamento de datas, nomes e tipos
- Criação de colunas derivadas

### Gold Layer
- Agregações de negócio
- KPIs:
  - Receita total
  - Custo total
  - Lucro
  - Ticket médio
  - Análises por cliente, produto e região

---

## 🚀 Deploy (CI/CD)

O projeto utiliza GitHub Actions para automação:

- Push na branch `dev` → deploy em DEV
- Push na branch `main` → deploy em PROD

Workflow executa:

```bash
databricks bundle deploy --target prod

## 📄 Licença

Este projeto é fornecido como está. Consulte com sua organização sobre os termos de uso e propriedade intelectual.

---

**Última atualização**: Maio 2026



## 📞 Suporte e Contato

- **Autor**: borge.pambo71@gmail.com
- **Workspace**: dbc-aab5aa5a-ee27.cloud.databricks.com
- **UUID do Bundle**: c1ae8260-609a-4f13-bb1c-d7f02032e1ed

-
