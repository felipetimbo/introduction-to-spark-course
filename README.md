# **Municurso - Introdução a Big Data com Apache Spark**

# 1 **Introdução ao Spark**

Para avançar nas suas habilidades com dados, é preciso dominar o Apache Spark. Usando a API do Spark para Pyhton, PySpark,você aproveitará a computação paralela com grandes datasets, e irá ficar pronto para alta performance limpando dados, criando novas featrures e implementando modelos de machine learning. 

O Apache Spark é uma alternativa poderosa ao Hadoop MapReduce, com recursos avançados como aprendizado de máquina, processamento de fluxo em tempo real e cálculos gráficos.

## 1.1 O que é Spark?

Spark é uma plataforma para computação em cluster. Spark permite que você espalhe permite distribuir dados e faz cálculos em clusters com vários nós (Pense em cada nó como um computador separado). Dividir seus dados facilita o trabalho com conjuntos de dados muito grandes porque cada nó funciona apenas com uma pequena quantidade de dados.

Na medida que cada nó trabalha em cada subset dos dados total, também realiza uma parte dos cálculos totais necessários, para que o processamento de dados e a computação sejam executados em paralelo nos nós do cluster. É um fato que a computação paralela pode tornar certos tipos de tarefas de programação muito mais rápidas.

No entanto, com maior poder de computação vem maior complexidade.

Decidir se o Spark é ou não a melhor solução para o seu problema requer alguma experiência, mas você pode considerar questões como:

- Meus dados são grandes demais para trabalhar em uma única máquina?
- Meus cálculos podem ser facilmente paralelizados?

# Instalando o PySpark no Google Colab
Instalar o PySpark não é um processo direto como de praxe em Python. Não basta usar um pip install apenas. Na verdade, antes de tudo é necessário instalar dependências como o **Java 8**, **Apache Spark 3.2.2** junto com o **Hadoop 2.7**.


```python
# instalar as dependências
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-3.2.2/spark-3.2.2-bin-hadoop2.7.tgz
!tar xf spark-3.2.2-bin-hadoop2.7.tgz
!pip install -q findspark
```

A próxima etapa é configurar as variáveis de ambiente, pois isso habilita o ambiente do Colab a identificar corretamente onde as dependências estão rodando.

Para conseguir “manipular” o terminal e interagir como ele, você pode usar a biblioteca os.


```python
# Configurar as variáveis de ambiente
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.2.2-bin-hadoop2.7"

# Torna o pyspark "importável"
import findspark
findspark.init('spark-3.2.2-bin-hadoop2.7')
```

Com tudo pronto, vamos rodar uma sessão local para testar se a instalação funcionou corretamente.

## 1.2 Usando Spark com Python

A primeira etapa no uso do Spark é conectar-se a um cluster.

Na prática, o cluster será hospedado em uma máquina remota conectada a todos os outros nós. Haverá um computador, chamado de mestre, que consegue dividir os dados e os cálculos. O mestre está conectado ao restante dos computadores do cluster, que são chamados de trabalhadores. O mestre envia os dados e cálculos dos trabalhadores para serem executados e eles enviam seus resultados de volta ao mestre.


### **Criando uma SparkSession**

A criação de vários `SparkSession`s e `SparkContext`s pode causar problemas, portanto, é uma prática recomendada usar o método `SparkSession.builder.getOrCreate()`. Isso retorna uma `SparkSession`  se já houver uma no ambiente ou cria uma nova, se necessário!


```python
# iniciar uma sessão local 
from pyspark.sql import SparkSession
#sc = SparkSession.builder.master('local[*]').getOrCreate()
spark = SparkSession.builder.appName("Introducao").getOrCreate()
```


```python
# Verify SparkContext
print(spark)

# Print Spark version
print(spark.version)
```

## 1.3 **Usando DataFrames**

A estrutura de dados principal do Spark é o Resilient Distributed Dataset (RDD). Este é um objeto de baixo nível que permite que o Spark faça sua mágica dividindo os dados em vários nós no cluster. No entanto, os RDDs são difíceis de trabalhar diretamente, portanto, nesta aula, você usará a abstração do Spark DataFrame criada com base nos RDDs.

O Spark DataFrame foi projetado para se comportar muito como uma tabela SQL (uma tabela com variáveis ​​nas colunas e observações nas linhas). Eles não são apenas mais fáceis de entender, mas os DataFrames também são mais otimizados para operações complicadas do que os RDDs.

Quando você começa a modificar e combinar colunas e linhas de dados, há muitas maneiras de chegar ao mesmo resultado, mas algumas geralmente demoram muito mais do que outras. Ao usar RDDs, cabe ao cientista de dados descobrir a maneira correta de otimizar a consulta, mas a implementação do DataFrame tem muito dessa otimização incorporada!

Para começar a trabalhar com o Spark DataFrames, primeiro você precisa criar um objeto `SparkSession` do seu `SparkContext`. Você pode pensar no `SparkContext` como sua conexão com o cluster e no `SparkSession` como sua interface com essa conexão.


###  **Como visualizar tabelas**

Depois de criar uma `SparkSession`, você pode começar a bisbilhotar para ver quais dados estão em seu cluster!

Sua `SparkSession` tem um atributo chamado `catalog` que lista todos os dados dentro do cluster. Este atributo possui alguns métodos para extrair diferentes informações.

Um dos mais úteis é o método `.listTables()` , que retorna os nomes de todas as tabelas em seu cluster como uma lista.


```python
# Print the tables in the catalog
print(spark.catalog.listTables())
```

### 1.3.3 **Importando tabelas & Fazendo query**

Uma das vantagens da interface DataFrame é que você pode executar consultas SQL nas tabelas em seu cluster Spark.

Iremos importar a tabela `flights` . Esta tabela contém uma linha para cada voo que saiu do Aeroporto Internacional de Portland (PDX) ou do Aeroporto Internacional de Seattle-Tacoma (SEA) em 2014 e 2015. Você pode baixar ela no repositório do github: https://github.com/felipetimbo/introduction-to-spark-course/

Vamos ver como executar uma consulta nesta tabela. Este método pega uma string contendo a consulta em SQL e retorna um DataFrame com os resultados!

Se você observar atentamente, perceberá que a tabela `flights` é mencionada apenas na consulta, não como um argumento para nenhum dos métodos. Isso ocorre porque não há um objeto local em seu ambiente que contenha esses dados, portanto, não faria sentido passar a tabela como um argumento.


```python
from google.colab import files
files.upload()
```


```python
arquivo = "flights_small.csv"
flights = spark\
        .read.format("csv")\
        .option("inferSchema", "True")\
        .option("header", "True")\
        .csv(arquivo)
```


```python
#Verificando o shape do pyspark dataframe
print((flights.count(), len(flights.columns)))
```


```python
flights.show(10)
```


```python
flights.printSchema()
```


```python
from pyspark.sql.functions import col
```


```python
#Retirando as datas e passando colunas para tipos corretos.
flights = flights.\
        withColumn("new_air_time", col("air_time").cast("integer")).drop("air_time")
```


```python
#renomeando colunas
flights = flights.withColumnRenamed("new_air_time","air_time")
```


```python
#Registrando o dataframe em uma view temporária
flights.createOrReplaceTempView("flights")

query = "FROM flights SELECT * LIMIT 10"

# Selecionando as 10 primeiras linhas do dataset
flights10 = spark.sql(query)

# Print o resultado
flights10.show()
```


```python
sqlDF = spark.sql("SELECT * FROM flights LIMIT 10")
sqlDF.show()
```

### Global Temporary View
As `Temporary views` (Como a que criamos acima) no Spark SQL têm escopo de sessão e desaparecerão se a sessão que a criou for encerrada. Se você quiser ter uma exibição temporária compartilhada entre todas as sessões e mantê-la ativa até que o aplicativo Spark seja encerrado, você pode criar uma exibição temporária global. A visão temporária global está vinculada a um banco de dados global_temp preservado pelo sistema e devemos usar o nome qualificado para referenciá-lo.


```python
# Registtrando o dataframe como view global
flights.createGlobalTempView("flights")

# A visão temporária global está vinculada a um banco de dados preservado pelo sistema `global_temp`
spark.sql("SELECT * FROM global_temp.flights LIMIT 10").show()
```

### 1.3.4 **Passando PySpark Dataframe para Pandas Dataframe**

Suponha que você executou uma consulta em seu enorme conjunto de dados e o agregou para algo um pouco mais gerenciável.

Às vezes, faz sentido pegar essa tabela e trabalhar com ela localmente usando uma ferramenta como `pandas`. O Spark DataFrames facilita isso com o método `.toPandas()` . Chamar esse método em um Spark DataFrame retorna o `pandas` DataFrame correspondente. É simples assim!

Desta vez, a consulta conta o número de voos para cada aeroporto de SEA e PDX.



```python
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Rodando a query
flight_counts = spark.sql(query)
```


```python
import pandas as pd
```


```python
# Convertendo o resultado para pandas
df = flights.toPandas()
```


```python
df.head()
```

No entanto, talvez você queira ir na outra direção e colocar um `pandas` DataFrame em um cluster Spark! A classe `SparkSession` tem um método para isso também.

O método `.createDataFrame()` pega um `pandas` DataFrame e retorna um Spark DataFrame.

A saída desse método é armazenada localmente, não no catálogo `SparkSession`. Isso significa que você pode usar todos os métodos do Spark DataFrame nele, mas não pode acessar os dados em outros contextos.

Por exemplo, uma consulta SQL (usando o método `.sql()` ) que faz referência ao seu DataFrame gerará um erro. Para acessar os dados dessa maneira, você deve salvá-los como uma *tabela temporária*.


```python
files.upload()
```


```python
# Criando pandas dataframe
arq = "airports.csv"
pd_temp = pd.read_csv(arq)
```


```python
pd_temp.head()
```


```python
# Cria spark_temp a partir de pd_temp
spark_temp = spark.createDataFrame(pd_temp)
```


```python
spark_temp.show(5)
```

## 2 **Manipulando Dados**

### 2.1 **Criando Colunas**

Aqui aprenderá a usar os métodos definidos pela classe `DataFrame` do Spark para realizar operações de dados comuns.

Vejamos a execução de operações em colunas. No Spark, você pode fazer isso usando o método `.withColumn()` , que recebe dois argumentos. Primeiro, uma string com o nome da sua nova coluna e depois a própria coluna.

A nova coluna deve ser um objeto da classe `Column`. Criar um deles é tão fácil quanto extrair uma coluna do seu DataFrame usando `df.colName`.

Atualizar um Spark DataFrame é um pouco diferente de trabalhar em `pandas` porque o Spark DataFrame é *imutável*. Isso significa que ele não pode ser alterado e, portanto, as colunas não podem ser atualizadas no local.

Assim, todos esses métodos retornam um novo DataFrame. Para substituir o DataFrame original, você deve reatribuir o DataFrame retornado usando o método da seguinte forma: `df = df.withColumn("newCol", df.oldCol + 1)`


```python
flights.select(flights.air_time/60).show()
```


```python
flights.show(10)
```


```python
# Add duration_hrs
flights = flights.withColumn("duration_hrs", flights.air_time/60)
```

retorna uma coluna de durações de voo em horas em vez de minutos. Você também pode usar o método .alias() para renomear uma coluna que está selecionando. Então, se você quisesse .select() a coluna duration_hrs (que não está no seu DataFrame), você poderia fazer


```python
flights.show()
```


```python
flights.select((flights.air_time/60).alias("duration_hrs")).show()
```

*Exercício: Altere a coluna `duration_hrs` do dataframe `flights` para arredondar os valores de tempo de duração em duas casas decimais.*


```python

```

# **Selecting**

A variante Spark do `SELECT` do SQL é o método `.select()`. Esse método recebe vários argumentos - um para cada coluna que você deseja selecionar. Esses argumentos podem ser o nome da coluna como uma string (uma para cada coluna) ou um objeto de coluna (usando a sintaxe `df.colName` ). Ao passar um objeto de coluna, você pode realizar operações como adição ou subtração na coluna para alterar os dados contidos nela, assim como em `.withColumn()`.

A diferença entre os métodos `.select()` e `.withColumn()` é que `.select()` retorna apenas as colunas que você especificar, enquanto `.withColumn()` retorna todas as colunas do DataFrame, além do um que você definiu. Muitas vezes, é uma boa ideia soltar as colunas que você não precisa no início de uma operação para que você não arraste dados extras enquanto lida. Nesse caso, você usaria `.select()` e não `.withColumn()`.

Semelhante ao SQL, você também pode usar o método .select() para realizar operações em colunas. Ao selecionar uma coluna usando a notação df.colName , você pode realizar qualquer operação de coluna e o método .select() retornará a coluna transformada. Por exemplo:


```python
# Selecionando um subconjunto do dataset
selected1 = flights.select("tailnum", "origin", "dest")
selected1.show(5)
```


```python
lista = ["tailnum", "origin", "dest"]
selected2 = flights.select(lista)
selected2.show(5)
```


```python
# Selecionando um subconjunto do dataset (outra maneira)
temp = flights.select(flights.origin, flights.dest, flights.carrier)
temp.show(5)
```

## **Filtrando dados**
Vamos dar uma olhada no método `.filter()` . Como você pode suspeitar, esta é a contrapartida do Spark da cláusula `WHERE` do SQL. O método `.filter()`  aceita uma expressão que seguiria a cláusula `WHERE` de uma expressão SQL como uma string ou uma coluna Spark de valores booleanos (`True`/`False`).

Por exemplo, as duas expressões a seguir produzirão a mesma saída:


```python
flights.filter("air_time > 120").show()
```


```python
flights.filter(flights.air_time > 120).show(5)
```

Observe que no primeiro caso, passamos uma string para .filter(). Em SQL, escreveríamos essa tarefa de filtragem como `SELECT * FROM flight WHERE air_time > 120`. 

O `.filter()` do Spark pode aceitar qualquer expressão que possa ir na cláusula WHERE de uma consulta SQL (neste caso, `"air_time > 120"`), desde que seja passado como uma string. Observe que, no primeiro caso, não fazemos referência ao nome da tabela na string - como não faríamos na solicitação SQL.

Também podemos fazer da seguinte forma:


```python
# Definindo o primeiro filtro
filterA = flights.origin == "SEA"

# Definindo o segundo filtro
filterB = flights.dest == "PDX"

# Filtrando os dados, primeiro pelo filterA entao pelo filterB
selected2 = selected2.filter(filterA).filter(filterB)
```


```python
selected2.show(10)
```

*Exercício: agora filtre o dataframe `selected2` para que a condição seja OU voos de origem em "SEA" OU voos com destino em "PDX".*


```python

```

# **Agregando**

Todos os métodos de agregação comuns, como `.min()`, `.max()` e `.count()` são métodos `GroupedData`. Eles são criados chamando o método `.groupBy()` DataFrame. Por enquanto, tudo o que você precisa fazer para usar essas funções é chamar esse método em seu DataFrame. Por exemplo, para encontrar o valor mínimo de uma coluna, `col`, em um DataFrame, `df`, você pode fazer `df.groupBy().min("col").show()`

### Encontre a duração do voo mais longo (em termos de tempo) que saiu do SEA  usando o método .max().


```python
# Achar a maior tempo de voo de SEA para outras cidades
flights.filter(flights.origin == "SEA").groupBy().max("duration_hrs").show()
```

### Encontre a menor distância percorrida que saiu do PDX  usando o método .min().


```python
# Achar a menor distancia do voo de PDX para outras cidades
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()
```

- Use o método `.avg()` para obter o tempo médio de voo dos voos da Delta Airlines (onde a coluna `transportadora` tem o valor `"DL"`) que saíram da SEA. O local de partida é armazenado na coluna `origem`. `show()` o resultado.
- Use o método `.sum()` para obter o número total de horas que todos os aviões neste conjunto de dados passaram no ar criando uma coluna chamada `duration_hrs` a partir da coluna `air_time`. `show()` o resultado.


```python
# Duração Média dos Voos da compania delta
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()
```


```python
# Tempo total em Horas no ar 
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()
```

Além dos métodos `GroupedData` que você já viu, há também o método `.agg()`. Esse método permite passar uma expressão de coluna agregada que usa qualquer uma das funções agregadas do submódulo `pyspark.sql.functions`.

Este submódulo contém muitas funções úteis para calcular coisas como desvio padrão. Todas as funções de agregação neste submódulo usam o nome de uma coluna em uma tabela `GroupedData`.


- Importe o submódulo `pyspark.sql.functions` como `F`.
- Crie uma tabela `GroupedData` chamada `by_month_dest` agrupada pelas colunas `month` e `dest` . Consulte as duas colunas passando ambas as strings como argumentos separados.
- Use o método `.avg()` no `by_month_dest` DataFrame para obter a média `dep_delay` em cada mês para cada destino.
- Encontre o desvio padrão de `dep_delay` usando o método `.agg()` com a função `F.stddev()`.


```python
# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# GroupBy por Mes e destino
by_month_dest = flights.groupBy("month", "dest")
```


```python
# Desvio Padrão 
by_month_dest.agg(F.stddev("dep_delay")).show()
```

*Exercício: Quantos voos da companhia WN pousaram em São Francisco "SFO" em maio de 2014?*


```python

```

# **Joining**

Outra operação de dados muito comum é a *join*. As junções são um tópico inteiro em si mesmas, portanto, neste curso, veremos apenas junções simples.

Um join combinará duas tabelas diferentes ao longo de uma coluna que elas compartilham. Essa coluna é chamada de *chave*. Exemplos de chaves aqui incluem as colunas `tailnum` e `carrier` da tabela `flights`.

Por exemplo, suponha que você queira saber mais informações sobre o avião que realizou um voo do que apenas o número da cauda. Essas informações não estão na tabela `voos` porque o mesmo avião realiza muitos voos diferentes ao longo de dois anos, portanto, incluir essas informações em todas as linhas resultaria em muita duplicação. Para evitar isso, você teria uma segunda tabela que possui apenas uma linha para cada plano e cujas colunas listam todas as informações sobre o avião, incluindo seu número de cauda. Você pode chamar essa tabela de `planes`.

Ao juntar a tabela `flights` a esta tabela de informações de avião, você está adicionando todas as colunas da tabela `planes` à tabela `flights`. Para preencher essas colunas com informações, você verá o número final da tabela `voos` e encontrará o número correspondente na tabela `aviões` e, em seguida, usará essa linha para preencher todas as novas colunas.

Agora você terá uma tabela muito maior do que antes, mas agora cada linha tem todas as informações sobre o avião que fez aquele voo!

No PySpark, as junções são realizadas usando o método DataFrame `.join()`. Este método recebe três argumentos.

- O primeiro é o segundo DataFrame que você deseja unir ao primeiro. 

- O segundo argumento, `on`, é o nome das colunas-chave como uma string. Os nomes da(s) coluna(s) chave devem ser os mesmos em cada tabela. 

- O terceiro argumento, `como`, especifica o tipo de junção a ser executada. Neste curso, sempre usaremos o valor `how="leftouter"`.

O conjunto de dados `flights` e o novo dataset `airports` já estão no seu workspace.

- Examine o `airports` DataFrame chamando `.show()`. Observe qual coluna de chave permitirá que você junte `aeroportos` à tabela `voos` .



```python
arquivo = "airports.csv"
airports = spark\
        .read.format("csv")\
        .option("inferSchema", "True")\
        .option("header", "True")\
        .csv(arquivo)
```


```python
# Examine the data
airports.show(10)
```


```python
flights.show(10)
```

Renomeie a coluna `faa` em `airports` para `dest` reatribuindo o resultado de `airports.withColumnRenamed("faa", "dest")` para `airports`.


```python
# Renomeie a coluna faa
airports = airports.withColumnRenamed("faa", "dest")
```


```python
airports.show()
```

Junte-se ao `flights` com o `airports` DataFrame na coluna `dest` chamando o método `.join()` em `flights`. Salve o resultado como `flights_with_airports`.
    - O primeiro argumento deve ser o outro DataFrame, `airports`.
    - O argumento `on` deve ser a coluna-chave.
    - O argumento `how` deve ser `"leftouter"`.


```python
# Join os DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")
```

Chame o método `.show()` em `flights_with_airports` para examinar os dados novamente. Observe as novas informações que foram adicionadas.


```python
# Examine the new DataFrame
flights_with_airports.show()
```

*Exercício: leia o novo dataset `planes.csv` em https://github.com/felipetimbo/introduction-to-spark-course contendo dados sobre as características dos aviões e converta-o em um dataframe.*


```python

```

*No dataframe `planes`, renomeie a coluna "year" para "plane_year".*


```python

```

*Faça um join do dataframe `flights` com o dataframe `planes` na coluna `tailnum` e mostra o resultado.*


```python

```

# FIM
