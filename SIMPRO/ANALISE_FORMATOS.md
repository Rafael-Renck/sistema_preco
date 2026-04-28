# Analise dos formatos SIMPRO

Data da analise: 2026-04-24

## Arquivos avaliados

- `MSG 17 2026 json.JSON`
- `MSG 17 2026 xml.XML`
- `MSG 17 2026 CSV.CSV`
- `MSG 17 2026 PONTO E VIRGULA.TXT`
- `MSG 17 2026 VIRGULA.TXT`
- `MSG 17 2026 BARRA VERTICAL.TXT`
- `MSG 17 2026 TABULACAO.TXT`
- `MSG 17 2026 NAO DELIMITADO.TXT`
- `MSG 17 2026 dataflex.TXT`
- `MSG 17 2026 dbase.DBF`
- `MSG 17 2026 paradox.DBF`

## Resumo executivo

Os melhores formatos para importacao sao:

1. `json.JSON`
2. `xml.XML`
3. `CSV.CSV`
4. `PONTO E VIRGULA.TXT`
5. `NAO DELIMITADO.TXT`

Os piores formatos para automacao sao:

- `dataflex.TXT`, porque nao tem largura fixa estavel
- `VIRGULA.TXT`, porque depende de locale e o separador pode conflitar com numeros em outros cenarios
- `DBF`, porque exige leitor especifico e nao traz vantagem clara sobre JSON/XML neste conjunto

## Evidencias observadas

### JSON

- 362 registros
- nomes de campos explicitos
- `codigoTUSS`, `anvisa`, `validadeAnvisa`, `codigoEAN`, `codigoSimpro` e `codigoUsuario` ja chegam separados
- numeros ja chegam tipados

Campos do primeiro registro:

`anvisa|classificacao|codigoEAN|codigoFracao|codigoMercado|codigoSimpro|codigoTUSS|codigoUsuario|desconto|descricao|diversos|embalagem|fabricante|fracao|fracionavel|generico|hospitalar|identificacao|ipi|lista|lucro|precoFabrica|precoFabricaFracao|precoUsuario|precoUsuarioFracao|precoVenda|precoVendaFracao|quantidadeEmbalagem|quantidadeFracao|referencia|tipoAlteracao|validadeAnvisa|vigencia`

### XML

- 362 registros
- mesma informacao do JSON
- atributos claros como `CD_TUSS`, `REGISTRO_ANVISA`, `CD_BARRA`, `CD_SIMPRO`
- exige parser XML, mas continua sendo formato confiavel

### Delimitados

Os arquivos `CSV.CSV`, `PONTO E VIRGULA.TXT`, `VIRGULA.TXT`, `BARRA VERTICAL.TXT` e `TABULACAO.TXT` tem:

- 33 campos por linha
- mesma ordem logica de dados
- o `CSV.CSV` e o `PONTO E VIRGULA.TXT` sao os mais seguros

### Nao delimitado

- cada linha possui 431 caracteres
- o layout eh estavel
- ainda assim depende de mapa de posicoes e eh mais sujeito a erro operacional

### Dataflex

- linhas com comprimentos variaveis
- parece formato de exportacao para leitura humana/sistema legado, nao um layout robusto para ETL

## Layout real do arquivo nao delimitado

As larguras derivadas do arquivo `PONTO E VIRGULA.TXT` batem exatamente com o `NAO DELIMITADO.TXT`.

| Campo | Inicio | Fim | Largura | Exemplo |
|---|---:|---:|---:|---|
| 01 | 1 | 15 | 15 | codigoUsuario |
| 02 | 16 | 30 | 15 | codigoFracao |
| 03 | 31 | 130 | 100 | descricao |
| 04 | 131 | 138 | 8 | vigencia |
| 05 | 139 | 139 | 1 | identificacao |
| 06 | 140 | 149 | 10 | precoFabrica |
| 07 | 150 | 159 | 10 | precoVenda |
| 08 | 160 | 169 | 10 | precoUsuario |
| 09 | 170 | 180 | 11 | precoFabricaFracao |
| 10 | 181 | 191 | 11 | precoVendaFracao |
| 11 | 192 | 202 | 11 | precoUsuarioFracao |
| 12 | 203 | 205 | 3 | embalagem |
| 13 | 206 | 209 | 4 | fracao |
| 14 | 210 | 216 | 7 | quantidadeEmbalagem |
| 15 | 217 | 223 | 7 | quantidadeFracao |
| 16 | 224 | 228 | 5 | lucro |
| 17 | 229 | 229 | 1 | tipoAlteracao |
| 18 | 230 | 249 | 20 | fabricante |
| 19 | 250 | 259 | 10 | codigoSimpro |
| 20 | 260 | 261 | 2 | codigoMercado |
| 21 | 262 | 266 | 5 | desconto |
| 22 | 267 | 271 | 5 | ipi |
| 23 | 272 | 289 | 18 | anvisa |
| 24 | 290 | 302 | 13 | validadeAnvisa |
| 25 | 303 | 315 | 13 | codigoEAN |
| 26 | 316 | 316 | 1 | lista |
| 27 | 317 | 317 | 1 | hospitalar |
| 28 | 318 | 318 | 1 | fracionavel |
| 29 | 319 | 327 | 9 | codigoTUSS |
| 30 | 328 | 329 | 2 | classificacao |
| 31 | 330 | 429 | 100 | referencia |
| 32 | 430 | 430 | 1 | generico |
| 33 | 431 | 431 | 1 | diversos |

## Problemas encontrados no pipeline atual

### 1. O importador de SIMPRO ainda esta orientado a largura fixa

Hoje o pipeline principal de SIMPRO trabalha em cima de `fixed-width`, mesmo quando existem formatos muito mais ricos e seguros no proprio pacote.

Impacto:

- maior dependencia de `map_config`
- maior custo de manutencao quando o layout muda
- mais chance de erro silencioso

### 2. O TUSS real ja existe como campo proprio, mas o parser atual tenta inferi-lo

Nos arquivos reais analisados, o TUSS esta pronto:

- JSON: `codigoTUSS`
- XML: `CD_TUSS`
- TXT fixo: posicao `319-327`

O parser atual tem heuristicas para extrair TUSS a partir de:

- `ean`
- trechos com prefixos como `NS`, `NN`, `SN`
- sufixo livre da linha

Impacto:

- isso pode gerar TUSS errado quando o codigo correto ja estava disponivel
- o sistema passa a depender de heuristica em vez de dado de origem

### 3. O campo `codigo` da tabela normalizada esta sendo usado como TUSS

Na implementacao atual, o payload de SIMPRO converte `codigo` para um valor formatado a partir de `tuss_numero`.

Impacto:

- perda do identificador principal do item SIMPRO
- dificuldade para reconciliar `codigoUsuario`, `codigoSimpro` e `codigoTUSS`
- risco de confundir codigo do item com codigo TUSS na busca e no indice

### 4. `validadeAnvisa` nao eh sempre uma data

No JSON real ha valores como:

- `30092028`
- `VIGENTE`
- vazio

Impacto:

- se a coluna for tratada apenas como data, parte da informacao some
- `VIGENTE` deveria virar um estado explicito, nao `NULL`

### 5. `anvisa` nao tem comprimento unico

Nos dados reais apareceram comprimentos:

- 6
- 11
- 13

Impacto:

- normalizar tudo para 13 digitos sem criterio pode corromper materiais e OPME
- o importador precisa guardar o bruto e, separadamente, uma versao normalizada quando aplicavel

## Recomendacao de estrategia

### Estrategia ideal

Usar uma hierarquia de importacao por confiabilidade:

1. JSON
2. XML
3. CSV delimitado com schema conhecido
4. TXT de largura fixa

### Modelo de dados recomendado

Separar claramente estes identificadores:

- `codigo_usuario`
- `codigo_fracao`
- `codigo_simpro`
- `codigo_tuss`
- `codigo_ean`

E evitar usar `codigo` como alias ambiguo.

### Regras recomendadas para TUSS

- usar `codigoTUSS` ou `CD_TUSS` como fonte primaria
- usar heuristica somente como fallback
- salvar o valor bruto e o valor normalizado
- manter TUSS sem misturar com o codigo principal do item

### Regras recomendadas para ANVISA

- salvar `anvisa_raw`
- salvar `anvisa_normalizada` apenas quando houver regra clara
- manter `validade_anvisa_raw`
- opcionalmente salvar `validade_anvisa_data` quando for data valida
- mapear `VIGENTE` como status, nao como data nula silenciosa

## Recomendacao pratica para o projeto

### Curto prazo

- priorizar importacao por `JSON`
- aceitar `XML` como segunda opcao
- manter `TXT fixo` apenas como fallback legado

### Medio prazo

- criar um importador `simpro:import-json`
- criar um importador `simpro:import-xml`
- revisar `simpro_item_norm` para nao usar `codigo` como TUSS
- incluir colunas brutas e normalizadas para TUSS e ANVISA

### Longo prazo

- unificar tudo em um pipeline por schema, nao por heuristica de linha
- validar cada lote com contagens de completude:
  - percentual com TUSS
  - percentual com ANVISA
  - percentual com EAN
  - percentual com validade ANVISA interpretada
  - divergencias entre codigo principal e codigo TUSS

## Conclusao

Para reduzir retrabalho e erro de interpretacao, a melhor escolha eh abandonar o TXT como fonte principal sempre que JSON ou XML estiverem disponiveis.

O maior risco atual nao esta no pacote SIMPRO que voce extraiu, e sim no fato de o pipeline ainda tentar reconstruir TUSS e ANVISA por heuristica, mesmo quando esses campos ja chegam prontos no arquivo.
